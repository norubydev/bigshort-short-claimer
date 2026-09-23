
from __future__ import annotations

import asyncio
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi.requests import AsyncSession
from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_hash.auto import keccak
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from web3 import AsyncHTTPProvider, AsyncWeb3, Web3

# =========================
# SETTINGS
# =========================
MODE = "claim"  # check | claim
THREADS = 20
RPC = "http://bsc.vision-node.com:7745"
COLLECT = ""  # YOUR wallet — receives claimed $short (required for claim)

CLAIM_PROXY = "0x0C348f3957d3426e18507d3FF105e9825fC80BA0"
TOKEN = "0x7C46508F0937A0C8Fb4C34fA20Fef70eC9a6BbBb"

BASE_DIR = Path(__file__).resolve().parent
STOP_EVENT = asyncio.Event()
stats = {"success": 0, "failed": 0, "skipped": 0, "eligible": 0}

logger.remove()
logger.level("DEBUG", color="<white>")
logger.level("INFO", color="<cyan>")
logger.level("SUCCESS", color="<green>")
logger.level("WARNING", color="<yellow>")
logger.level("ERROR", color="<red>")
logger.add(
    BASE_DIR / "logger.log",
    format="{time:YYYY-MM-DD | HH:mm:ss.SSS} | {level.name: <7} | {function}:{line} | {extra[wallet]} - {message}",
    rotation="10 MB",
)


def level_color(record):
    return (
        "<blue>{time:YYYY-MM-DD | HH:mm:ss.SSS}</blue> | "
        "<level>{level.name: <7}</level> | "
        "<magenta>{extra[wallet]}</magenta> - "
        "<level>{message}</level>\n"
    )


logger.add(
    sys.stdout,
    filter=lambda record: record["level"].name in ("DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR"),
    format=level_color,
    colorize=True,
)
logger = logger.bind(wallet="SYSTEM")


class TemporaryError(Exception):
    pass


class TerminalError(Exception):
    pass


class RateLimited(TemporaryError):
    pass


def transient_retry(attempts: int = 5):
    return retry(
        retry=retry_if_exception_type(
            (TemporaryError, RateLimited, asyncio.TimeoutError, ConnectionError, OSError)
        ),
        stop=stop_after_attempt(attempts),
        wait=wait_fixed(1),
        reraise=True,
    )


def project_path(path: str) -> Path:
    return BASE_DIR / path


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_lines(path: str) -> list[str]:
    try:
        with open(project_path(path), "r", encoding="utf-8-sig") as f:
            return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    except FileNotFoundError:
        return []


async def append_line_locked(lock: asyncio.Lock, path: str, line: str):
    async with lock:
        full = project_path(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        with open(full, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")


async def sleepy():
    await asyncio.sleep(random.uniform(0, 1))


def parse_wallet(raw: str) -> tuple[Account | None, str]:
    line = raw.strip().split("|")[0].strip()
    if ":" in line and not line.startswith("0x"):
        line = line.split(":")[0].strip()
    if line.startswith("0x") and len(line) == 42:
        return None, Web3.to_checksum_address(line)
    key = line if line.startswith("0x") else "0x" + line
    acc = Account.from_key(key)
    return acc, Web3.to_checksum_address(acc.address)


def normalize_proxy(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("http://") or line.startswith("https://"):
        return line
    # host;port;user;pass
    if ";" in line:
        parts = line.split(";")
        if len(parts) == 4:
            host, port, user, password = parts
            return f"http://{user}:{password}@{host}:{port}"
        if len(parts) == 2:
            host, port = parts
            return f"http://{host}:{port}"
    parts = line.split(":")
    if len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    if len(parts) == 4:
        ip, port, login, password = parts
        return f"http://{login}:{password}@{ip}:{port}"
    raise ValueError(f"Bad proxy format: {line}")


def load_proxies(path: str) -> list[str]:
    out = []
    for line in read_lines(path):
        p = normalize_proxy(line)
        if p:
            out.append(p)
    return out


def get_random_proxy(proxies: list[str]) -> str | None:
    return random.choice(proxies) if proxies else None


def short_amount(raw: str | int | None) -> str:
    if raw is None:
        return "0"
    return str(int(raw) // 10**18)


def load_done() -> set[str]:
    done: set[str] = set()
    for path in ("logs/success.txt", "logs/skipped.txt"):
        for line in read_lines(path):
            parts = line.split("\t")
            if len(parts) >= 3:
                done.add(parts[2].strip().lower())
    return done


@dataclass
class QueueTask:
    idx: int
    total: int
    raw: str
    attempts: int = 0


@dataclass
class Locks:
    success: asyncio.Lock
    failed: asyncio.Lock
    skipped: asyncio.Lock
    result: asyncio.Lock
    eligible: asyncio.Lock
    funder: asyncio.Lock


class Bot:
    def __init__(
        self,
        task: QueueTask,
        session: AsyncSession,
        locks: Locks,
        funder: Account | None,
        w3: AsyncWeb3,
        proxy: str | None,
    ):
        self.task = task
        self.session = session
        self.locks = locks
        self.funder = funder
        self.w3 = w3
        self.proxy = proxy
        self.acc: Account | None = None
        self.address = ""
        self.csrf = ""
        self.log = logger.bind(wallet=f"[{task.idx}/{task.total}] ?")

    def headers(self, csrf: bool = True) -> dict:
        h = {
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://airdrop.bigshort.xyz",
            "referer": "https://airdrop.bigshort.xyz/?ref=EVM-Q370",
            "Idempotency-Key": str(uuid.uuid4()),
        }
        if csrf and self.csrf:
            h["X-CSRF-Token"] = self.csrf
        return h

    @transient_retry()
    async def http(self, method: str, url: str, **kw):
        await sleepy()
        try:
            r = await self.session.request(
                method,
                url,
                headers=kw.pop("headers", self.headers()),
                proxy=self.proxy,
                impersonate="chrome110",
                timeout=30,
                verify=False,
                **kw,
            )
        except (asyncio.TimeoutError, ConnectionError, OSError) as e:
            raise TemporaryError(str(e)) from e
        except Exception as e:
            err = str(e).lower()
            if "proxy" in err or "curl" in err or "timeout" in err or "connect" in err:
                raise TemporaryError(str(e)) from e
            raise
        if r.status_code == 429:
            raise RateLimited("429")
        if r.status_code in (500, 502, 503, 504):
            raise TemporaryError(f"server {r.status_code}")
        return r

    async def refresh_csrf(self):
        r = await self.http("GET", "https://airdrop.bigshort.xyz/api/v1/auth/session", headers=self.headers(csrf=False))
        if r.status_code != 200:
            raise TemporaryError(f"session {r.status_code}: {r.text[:200]}")
        self.csrf = r.json()["data"]["csrfToken"]

    async def eligibility(self) -> dict:
        url = (
            "https://airdrop.bigshort.xyz/api/v1/airdrops/airdrop-20260920-v1/eligibility"
            f"?namespace=evm&address={self.address}"
        )
        r = await self.http("GET", url, headers={**self.headers(csrf=False), "accept": "*/*"})
        if r.status_code != 200:
            raise TemporaryError(f"elig {r.status_code}: {r.text[:200]}")
        return r.json()["data"]

    async def login(self):
        assert self.acc
        await self.refresh_csrf()
        body = {
            "namespace": "evm",
            "address": self.address,
            "purpose": "login",
            "chainId": 56,
        }
        r = await self.http("POST", "https://airdrop.bigshort.xyz/api/v1/auth/challenge", json=body)
        if r.status_code != 200:
            raise TemporaryError(f"challenge {r.status_code}: {r.text[:200]}")
        data = r.json()["data"]
        signed = self.acc.sign_message(encode_defunct(text=data["message"]))
        sig = "0x" + signed.signature.hex()
        r = await self.http(
            "POST",
            "https://airdrop.bigshort.xyz/api/v1/auth/verify",
            json={"challengeId": data["challengeId"], "signature": sig},
        )
        if r.status_code != 200:
            raise TerminalError(f"verify {r.status_code}: {r.text[:200]}")
        await self.refresh_csrf()
        self.log.info("logged in")

    async def bind_ref(self):
        r = await self.http(
            "POST",
            "https://airdrop.bigshort.xyz/api/v1/airdrops/airdrop-20260920-v1/referrals/bind",
            json={"identity": {"namespace": "evm", "address": self.address}, "code": "EVM-Q370"},
        )
        if r.status_code == 200:
            self.log.info("referral bound")
        else:
            self.log.warning(f"bind skip {r.status_code}: {r.text[:120]}")

    async def prepare_confirm(self) -> str:
        assert self.acc
        identity = {"namespace": "evm", "address": self.address}
        r = await self.http(
            "POST",
            "https://airdrop.bigshort.xyz/api/v1/airdrops/airdrop-20260920-v1/claim/intents",
            json={"identity": identity, "kind": "base", "recipient": self.address},
        )
        if r.status_code != 200:
            raise TerminalError(f"intent {r.status_code}: {r.text[:300]}")
        intent = r.json()["data"]
        signed = self.acc.sign_message(encode_defunct(text=intent["message"]))
        sig = "0x" + signed.signature.hex()
        r = await self.http(
            "POST",
            "https://airdrop.bigshort.xyz/api/v1/airdrops/airdrop-20260920-v1/claim/confirm",
            json={
                "identity": identity,
                "intentId": intent["id"],
                "signature": sig,
                "outcome": "success",
            },
        )
        if r.status_code != 200:
            raise TerminalError(f"confirm {r.status_code}: {r.text[:300]}")
        rid = r.json()["data"]["id"]
        self.log.info(f"confirmed pending receipt={rid}")
        return rid

    async def authorize(self, receipt_id: str) -> dict:
        r = await self.http(
            "POST",
            "https://airdrop.bigshort.xyz/api/v1/airdrops/airdrop-20260920-v1/claim/authorize",
            json={
                "identity": {"namespace": "evm", "address": self.address},
                "claimant": self.address,
                "receiptId": receipt_id,
            },
        )
        if r.status_code == 409 and "DELIVERY_RECEIPT_REQUIRED" in r.text:
            raise TerminalError(f"DELIVERY_RECEIPT_REQUIRED: {r.text[:300]}")
        if r.status_code != 200:
            raise TemporaryError(f"authorize {r.status_code}: {r.text[:300]}")
        return r.json()["data"]

    async def report_receipt(self, tx_hash: str):
        r = await self.http(
            "POST",
            "https://airdrop.bigshort.xyz/api/v1/airdrops/airdrop-20260920-v1/claim/receipt",
            json={
                "identity": {"namespace": "evm", "address": self.address},
                "claimant": self.address,
                "hash": tx_hash,
            },
        )
        if r.status_code != 200:
            self.log.warning(f"receipt report {r.status_code}: {r.text[:160]}")

    async def token_balance(self, who: str) -> int:
        sel = keccak(b"balanceOf(address)")[:4]
        data = "0x" + sel.hex() + encode(["address"], [Web3.to_checksum_address(who)]).hex()
        raw = await self.w3.eth.call({"to": TOKEN, "data": data})
        return int(raw.hex(), 16)

    async def ensure_funded(self):
        bal = await self.w3.eth.get_balance(self.address)
        min_wei = int(0.00015 * 1e18)
        if bal >= min_wei:
            self.log.info(f"bnb ok {bal / 1e18:.6f}")
            return
        if not self.funder:
            raise TerminalError("INSUFFICIENT_FUNDS and no funder")
        amount = int(0.0004 * 1e18)
        self.log.info("funding 0.0004 BNB")
        async with self.locks.funder:
            bal = await self.w3.eth.get_balance(self.address)
            if bal >= min_wei:
                self.log.info(f"bnb ok after wait {bal / 1e18:.6f}")
                return
            tx_hash = await self.send_tx(self.funder, self.address, amount, b"", gas=21_000)
            await self.wait_ok(tx_hash)
        self.log.info(f"funded {tx_hash}")

    async def send_tx(self, signer: Account, to: str, value: int, data: bytes, gas: int | None = None) -> str:
        nonce = await self.w3.eth.get_transaction_count(signer.address, "pending")
        gas_price = int(await self.w3.eth.gas_price * 1.25)
        to_cs = Web3.to_checksum_address(to)
        tx = {
            "chainId": 56,
            "nonce": nonce,
            "to": to_cs,
            "value": value,
            "data": data,
            "gasPrice": gas_price,
            "from": signer.address,
        }
        if gas is None:
            self.log.info("Estimating gas")
            try:
                gas = int(await self.w3.eth.estimate_gas(tx) * 1.20)
            except Exception as e:
                raise TemporaryError(f"estimate fail: {e}") from e
            self.log.info(f"Gas estimate: {gas}")
        tx["gas"] = gas
        signed = signer.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or signed.rawTransaction
        self.log.info("Sending tx")
        try:
            tx_hash = await self.w3.eth.send_raw_transaction(raw)
        except Exception as e:
            msg = str(e).lower()
            if "nonce" in msg or "underpriced" in msg or "already known" in msg:
                raise TemporaryError(f"send_tx: {e}") from e
            raise
        hx = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
        if not hx.startswith("0x"):
            hx = "0x" + hx
        self.log.info(f"Tx sent: {hx}")
        return hx

    async def wait_ok(self, tx_hash: str, timeout: int = 180):
        self.log.info(f"Waiting receipt: {tx_hash}")
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                rcpt = await self.w3.eth.get_transaction_receipt(tx_hash)
            except Exception as e:
                if "indexing" in str(e).lower():
                    await asyncio.sleep(1.2)
                    continue
                raise TemporaryError(f"receipt: {e}") from e
            if rcpt is not None:
                if int(rcpt["status"]) != 1:
                    raise TerminalError(f"TX_REVERTED {tx_hash}")
                return rcpt
            await asyncio.sleep(1.2)
        raise TemporaryError(f"TX_PENDING_TIMEOUT {tx_hash}")

    async def onchain_claim(self, auth: dict) -> str:
        assert self.acc
        amount = int(auth["amount"])
        deadline = int(auth["deadline"])
        sig = bytes.fromhex(auth["signature"].replace("0x", ""))
        sel = keccak(b"claim(uint256,uint256,bytes)")[:4]
        data = sel + encode(["uint256", "uint256", "bytes"], [amount, deadline, sig])
        hx = await self.send_tx(self.acc, CLAIM_PROXY, 0, data)
        await self.wait_ok(hx)
        self.log.success(f"Claimed {hx}")
        return hx

    async def collect(self) -> str:
        assert self.acc
        bal = await self.token_balance(self.address)
        if bal == 0:
            self.log.warning("no SHORT to collect")
            return "0"
        human = short_amount(bal)
        self.log.info(f"collecting {human} SHORT -> {COLLECT}")
        sel = keccak(b"transfer(address,uint256)")[:4]
        data = sel + encode(
            ["address", "uint256"], [Web3.to_checksum_address(COLLECT), bal]
        )
        hx = await self.send_tx(self.acc, TOKEN, 0, data)
        await self.wait_ok(hx)
        self.log.success(f"collected {human} {hx}")
        return human

    async def sweep_bnb(self) -> str | None:
        assert self.acc
        if not self.funder:
            self.log.warning("no funder — skip BNB sweep")
            return None
        to = self.funder.address
        if to.lower() == self.address.lower():
            return None
        bal = await self.w3.eth.get_balance(self.address)
        gas_price = int(await self.w3.eth.gas_price * 1.25)
        fee = 21_000 * gas_price
        if bal <= fee:
            self.log.info(f"bnb dust too small to sweep ({bal})")
            return None
        value = bal - fee
        self.log.info(f"sweeping {value / 1e18:.8f} BNB -> {to}")
        hx = await self.send_tx(self.acc, to, value, b"", gas=21_000)
        await self.wait_ok(hx)
        self.log.success(f"bnb swept {hx}")
        return hx

    async def run(self):
        self.acc, self.address = parse_wallet(self.task.raw)
        self.log = logger.bind(wallet=f"[{self.task.idx}/{self.task.total}] {self.address}")

        data = await self.eligibility()
        status = (data.get("eligibility") or {}).get("status") or "unknown"
        claimed = data.get("claimed")
        alloc = data.get("allocation") or {}
        amount = short_amount(alloc.get("currentRaw") or alloc.get("estimatedRaw") or "0")

        if MODE == "check":
            if status == "eligible" and not claimed:
                line = f"{utc_now()}\tELIGIBLE\t{self.address}\t{amount}"
                await append_line_locked(self.locks.success, "logs/success.txt", line)
                await append_line_locked(self.locks.eligible, "results/eligible.txt", f"{self.address}\t{amount}")
                await append_line_locked(self.locks.result, "results/result.txt", line)
                stats["success"] += 1
                stats["eligible"] += 1
                self.log.success(f"ELIGIBLE ~{amount} $short")
                return
            settle = (claimed or {}).get("settlementMode") if claimed else None
            line = f"{utc_now()}\t{status}\t{self.address}\tclaimed={settle}\t{amount}"
            await append_line_locked(self.locks.skipped, "logs/skipped.txt", line)
            await append_line_locked(self.locks.result, "results/result.txt", line)
            stats["skipped"] += 1
            self.log.info(f"{status} claimed={settle}")
            return

        if not self.acc:
            raise TerminalError("claim needs private key")

        bal = await self.token_balance(self.address)
        if bal > 0:
            await self.ensure_funded()
            human = await self.collect()
            sweep = await self.sweep_bnb()
            line = f"{utc_now()}\tCOLLECT\t{self.address}\t{human}\tsweep={sweep or '-'}"
            await append_line_locked(self.locks.success, "logs/success.txt", line)
            await append_line_locked(self.locks.result, "results/result.txt", line)
            stats["success"] += 1
            return

        settle = (claimed or {}).get("settlementMode") if claimed else None
        if claimed and settle and settle != "pending":
            await self.ensure_funded()
            try:
                if await self.token_balance(self.address) > 0:
                    human = await self.collect()
                else:
                    human = "0"
            except Exception as e:
                self.log.warning(f"collect after settled skip: {e}")
                human = "0"
            sweep = await self.sweep_bnb()
            line = f"{utc_now()}\tALREADY_DONE\t{self.address}\tsettle={settle}\tcollected={human}\tsweep={sweep or '-'}"
            await append_line_locked(self.locks.skipped, "logs/skipped.txt", line)
            await append_line_locked(self.locks.result, "results/result.txt", line)
            stats["skipped"] += 1
            self.log.warning(f"already settled ({settle})")
            return

        if claimed and settle == "pending" and claimed.get("txHash"):
            await self.login()
            await self.report_receipt(claimed["txHash"])
            await self.ensure_funded()
            human = "0"
            try:
                if await self.token_balance(self.address) > 0:
                    human = await self.collect()
            except Exception as e:
                self.log.error(f"collect after receipt: {e}")
            sweep = await self.sweep_bnb()
            line = (
                f"{utc_now()}\tRECEIPT_REPORTED\t{self.address}\t"
                f"{claimed['txHash']}\tcollected={human}\tsweep={sweep or '-'}"
            )
            await append_line_locked(self.locks.success, "logs/success.txt", line)
            await append_line_locked(self.locks.result, "results/result.txt", line)
            stats["success"] += 1
            self.log.success(f"receipt reported {claimed['txHash']} collected={human}")
            return

        await self.login()
        await self.bind_ref()

        if claimed and settle == "pending":
            receipt_id = claimed["id"]
            self.log.info(f"resume pending {receipt_id}")
        else:
            if status != "eligible":
                line = f"{utc_now()}\tNOT_ELIGIBLE\t{self.address}\tstatus={status}"
                await append_line_locked(self.locks.skipped, "logs/skipped.txt", line)
                await append_line_locked(self.locks.result, "results/result.txt", line)
                stats["skipped"] += 1
                self.log.info(f"not eligible ({status})")
                return
            await self.ensure_funded()
            receipt_id = await self.prepare_confirm()

        await self.ensure_funded()
        auth = await self.authorize(receipt_id)
        tx_hash = await self.onchain_claim(auth)
        await self.report_receipt(tx_hash)
        await append_line_locked(
            self.locks.success,
            "logs/claimed_tx.txt",
            f"{utc_now()}\tCLAIMED\t{self.address}\t{tx_hash}",
        )
        await self.ensure_funded()
        try:
            human = await self.collect()
        except Exception as e:
            self.log.error(f"collect failed after claim: {e}")
            line = f"{utc_now()}\tCLAIMED_NO_COLLECT\t{self.address}\t{tx_hash}\t{e}"
            await append_line_locked(self.locks.success, "logs/success.txt", line)
            await append_line_locked(self.locks.result, "results/result.txt", line)
            stats["success"] += 1
            return
        sweep = await self.sweep_bnb()
        line = f"{utc_now()}\tSUCCESS\t{self.address}\t{tx_hash}\tcollected={human}\tsweep={sweep or '-'}"
        await append_line_locked(self.locks.success, "logs/success.txt", line)
        await append_line_locked(self.locks.result, "results/result.txt", line)
        stats["success"] += 1


async def save_failed(task: QueueTask, error: str, locks: Locks):
    try:
        _, address = parse_wallet(task.raw)
    except Exception:
        address = task.raw[:66]
    await append_line_locked(
        locks.failed, "logs/failed.txt", f"{utc_now()}\tFAILED\t{address}\t{error}\t{task.raw}"
    )
    stats["failed"] += 1


async def worker(
    queue: asyncio.Queue,
    locks: Locks,
    funder: Account | None,
    w3: AsyncWeb3,
    proxies: list[str],
):
    while not STOP_EVENT.is_set():
        try:
            task = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            if queue.empty():
                return
            continue
        try:
            proxy = get_random_proxy(proxies)
            async with AsyncSession() as session:
                await Bot(task, session, locks, funder, w3, proxy).run()
        except TemporaryError as e:
            if task.attempts < 3:
                task.attempts += 1
                logger.warning(f"Requeue {task.idx}: {e}")
                queue.put_nowait(task)
            else:
                logger.error(f"Max attempts {task.idx}: {e}")
                await save_failed(task, str(e), locks)
        except TerminalError as e:
            logger.error(f"Terminal {task.idx}: {e}")
            await save_failed(task, str(e), locks)
        except Exception as e:
            logger.exception(f"Unknown {task.idx}: {e}")
            await save_failed(task, str(e), locks)
        finally:
            queue.task_done()
            await sleepy()


def require_collect():
    raw = (COLLECT or "").strip()
    zero = "0x0000000000000000000000000000000000000000"
    if not raw or raw.lower() in ("0x", zero.lower()):
        logger.error(
            "COLLECT address not set — put YOUR receive wallet in COLLECT "
            "(main.py SETTINGS) and restart. Soft will not claim into thin air."
        )
        sys.exit(1)
    try:
        Web3.to_checksum_address(raw)
    except Exception:
        logger.error(f"COLLECT is not a valid address: {raw}")
        sys.exit(1)


async def main():
    for d in ("logs", "results", "accounts"):
        project_path(d).mkdir(parents=True, exist_ok=True)

    global MODE
    if len(sys.argv) > 1:
        MODE = sys.argv[1]
    logger.info("created by @deadcells_eth  https://x.com/deadcells_eth")
    logger.info(f"MODE={MODE}")

    if MODE == "claim":
        require_collect()
        logger.info(f"COLLECT {Web3.to_checksum_address(COLLECT.strip())}")

    proxies = load_proxies("accounts/proxies.txt")
    if proxies:
        logger.info(f"Loaded {len(proxies)} proxies")
    else:
        logger.warning("accounts/proxies.txt empty — direct IP")

    wallets_file = sys.argv[2] if len(sys.argv) > 2 else "accounts/wallets.txt"
    raws = read_lines(wallets_file)
    if not raws:
        logger.error(f"{wallets_file} empty")
        return
    logger.info(f"wallets={wallets_file} n={len(raws)}")

    funder = None
    if MODE == "claim":
        flines = read_lines("accounts/funder.txt")
        if flines:
            funder, faddr = parse_wallet(flines[0])
            logger.info(f"funder {faddr}")
        else:
            logger.warning("no funder.txt — wallets must have BNB")

    w3 = AsyncWeb3(AsyncHTTPProvider(RPC, request_kwargs={"timeout": 30}))
    done: set[str] = set()
    if MODE == "check":
        done = load_done()
    else:
        for line in read_lines("logs/success.txt"):
            parts = line.split("\t")
            if len(parts) >= 3:
                done.add(parts[2].strip().lower())
        for raw in read_lines("accounts/claimed_recovery.txt"):
            try:
                _, addr = parse_wallet(raw)
                done.add(addr.lower())
            except Exception:
                continue
    if done:
        logger.info(f"skip already-done addresses: {len(done)}")
    tasks: list[QueueTask] = []
    for i, raw in enumerate(raws, 1):
        try:
            _, addr = parse_wallet(raw)
        except Exception as e:
            logger.error(f"bad line [{i}]: {e}")
            continue
        if addr.lower() in done:
            continue
        tasks.append(QueueTask(i, len(raws), raw))

    if not tasks:
        logger.warning("nothing to do")
        return

    locks = Locks(
        success=asyncio.Lock(),
        failed=asyncio.Lock(),
        skipped=asyncio.Lock(),
        result=asyncio.Lock(),
        eligible=asyncio.Lock(),
        funder=asyncio.Lock(),
    )
    queue: asyncio.Queue = asyncio.Queue()
    for t in tasks:
        queue.put_nowait(t)

    workers = [
        asyncio.create_task(worker(queue, locks, funder, w3, proxies))
        for _ in range(min(THREADS, len(tasks)))
    ]
    try:
        await queue.join()
    finally:
        STOP_EVENT.set()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    logger.success(
        f"Done eligible={stats['eligible']} ok={stats['success']} "
        f"fail={stats['failed']} skip={stats['skipped']}"
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted")
