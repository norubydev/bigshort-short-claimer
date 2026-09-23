# BigShort `$short` Claimer

> created by [@deadcells_eth](https://x.com/deadcells_eth)

Check eligibility, claim on BSC, collect `$short`, sweep leftover BNB.

**Repo:** [norubydev/bigshort-short-claimer](https://github.com/norubydev/bigshort-short-claimer)

## Install

```bash
pip install -r requirements.txt
```

## Files

| path | purpose |
|---|---|
| `accounts/wallets.txt` | private keys, one per line (`check` also accepts addresses) |
| `accounts/funder.txt` | one key with BNB — tops up gas for claim/collect, leftover BNB is swept back |
| `accounts/proxies.txt` | HTTP proxies (optional) |
| `logs/` | `success.txt` / `skipped.txt` / `failed.txt` |
| `results/result.txt` | run summary |

Copy the examples:

```bash
cp accounts/wallets.example.txt accounts/wallets.txt
cp accounts/funder.example.txt accounts/funder.txt
cp accounts/proxies.example.txt accounts/proxies.txt
```

Set **`COLLECT`** in `main.py` to **your** wallet — that is where `$short` is sent.  
Claim mode **refuses to start** if `COLLECT` is empty.

```python
COLLECT = "0xYourWalletHere"
```

Set **`RPC`** to any working BSC HTTP endpoint (chain id `56`). Default is a public Binance seed:

```python
RPC = "https://bsc-dataseed.binance.org"
```

Other public options if that one is slow / rate-limited:

```text
https://bsc-dataseed1.binance.org
https://bsc-dataseed2.binance.org
https://bsc.publicnode.com
https://rpc.ankr.com/bsc
```

For big runs a private / paid BSC node is better. RPC is used for claim / collect / gas only — airdrop API goes through proxies (or direct IP).

## Run

```bash
python main.py check
python main.py claim
python main.py claim accounts/wallets.txt
```

Settings at the top of `main.py`: `MODE`, `THREADS`, `RPC`, `COLLECT`.

## Proxies

Residential proxies work. Get some here: [birdproxies.com/@deadcell](https://birdproxies.com/@deadcell)

**Large wallet lists** — use proxies (and keep threads reasonable).  
**Small lists** — you can run without proxies; leave `proxies.txt` empty and use fewer threads.

Supported formats (one per line):

```text
host:port:user:pass
http://user:pass@host:port
host;port;user;pass
host:port
```

BirdProxies `host:port:user:pass` lines work as-is.

Proxies are used for the airdrop API only. BSC RPC goes direct via `RPC`.

## Claim flow

1. Eligibility check  
2. If the wallet already holds `$short` → fund gas → transfer to `COLLECT` → sweep BNB to funder  
3. Else login → bind referral → authorize → on-chain claim → report receipt → collect → sweep  
4. Not eligible → `skipped` (not failed)

Re-runs skip addresses already present in `logs/success.txt`.
