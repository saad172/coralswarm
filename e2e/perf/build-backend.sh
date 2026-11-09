#!/usr/bin/env bash
# Debug e2e only. build.rs MUST continue to reject e2e at any opt-level >0.
# Pin each executable before the next feature-changing Cargo invocation.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${E2E_BACKEND_BIN_DIR:?set an owned binary pin directory}"
: "${PAGE_LOAD_EVIDENCE_DIR:?set an owned evidence directory}"
mkdir -p "$E2E_BACKEND_BIN_DIR" "$PAGE_LOAD_EVIDENCE_DIR"
export CORALSWARM_E2E_BUILD=1
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_INCREMENTAL=0
export SOURCE_COMMIT
SOURCE_COMMIT=$(git rev-parse HEAD)
build_and_pin() {
  local binary="$1"
  shift
  cargo build --locked -p coralswarm-backend "$@" --bin "$binary" --message-format=json \
    > "$PAGE_LOAD_EVIDENCE_DIR/build-$binary.jsonl"
  python3 - "$binary" "$PAGE_LOAD_EVIDENCE_DIR/build-$binary.jsonl" "$E2E_BACKEND_BIN_DIR" <<'PY'
import hashlib,json,pathlib,shutil,sys
name,log,pins=sys.argv[1:]
artifacts=[x for line in pathlib.Path(log).read_text().splitlines() if (x:=json.loads(line)).get('reason')=='compiler-artifact' and x['target']['name']==name and x.get('executable')]
if len(artifacts)!=1: raise SystemExit(f'expected one exact executable artifact for {name}')
a=artifacts[0]
features=a['features']
if ('e2e' in features)!=(name=='coralswarm-backend'): raise SystemExit(f'wrong feature set for {name}')
if a['profile']['opt_level']!='0': raise SystemExit('nightly requires ordinary unoptimized debug artifacts')
dest=pathlib.Path(pins)/name
source=pathlib.Path(a['executable'])
if dest.resolve()==source.resolve(): raise SystemExit('pin directory must be independent of Cargo output')
shutil.copy2(source,dest)
receipt={'binary':name,'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),'features':features,'profile':a['profile'],'source_executable':str(source)}
(pathlib.Path(log).parent/f'pin-{name}.json').write_text(json.dumps(receipt,indent=2)+'\n')
PY
}
build_and_pin coralswarm-backend --features e2e
build_and_pin migrate_control
build_and_pin migrate_tenants
python3 - "$PAGE_LOAD_EVIDENCE_DIR" <<'PY'
import hashlib,json,pathlib,subprocess,sys
root=pathlib.Path.cwd()
files=subprocess.check_output(['git','ls-files','backend','packages/core','Cargo.toml','Cargo.lock']).decode().splitlines()
hashes={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in files if (root/name).is_file()}
receipt={'head':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'tree':subprocess.check_output(['git','rev-parse','HEAD^{tree}']).decode().strip(),'diff_sha256':hashlib.sha256(subprocess.check_output(['git','diff','HEAD','--binary'])).hexdigest(),'profile':'dev opt0; backend e2e/default; both migrators independently default (no e2e)','source_sha256':hashes,'sql_sha256':{k:v for k,v in hashes.items() if k.endswith('.sql')},'rustc':subprocess.check_output(['rustc','-vV']).decode()}
(pathlib.Path(sys.argv[1])/'source-build.json').write_text(json.dumps(receipt,indent=2)+'\n')
PY
