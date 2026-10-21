#!/usr/bin/env bash
#
# Community edition boundary check (packaged artifact).
#
# Complements the CommunityEditionSeparationSpec (which checks the registry on the community *test*
# classpath) by inspecting the actually-shipped community fat jar and the CLI's runtime behavior:
#
#   jar content  — no enterprise library (Kafka, Delta, cron4j, Logstash) got shaded in.
#   SPI manifest — the merged META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension lists exactly
#                  the community extensions.
#   behavior     — the community CLI refuses enterprise jobs with an actionable MissingX error
#                  (streaming -> MissingCapabilityException, Kafka -> MissingConnectorException).
#
# The enterprise modules live in their own (private) repository, which checks the server jar's side of
# the boundary. Builds the community jar (tests skipped). Usage: test-flow/check-editions.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_JAR="$REPO_ROOT/ignifyr-cli/target/ignifyr-engine-standalone.jar"

PASS=0; FAILC=0
ok()   { printf '  \033[1;32mPASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '  \033[1;31mFAIL\033[0m %s\n' "$*"; FAILC=$((FAILC+1)); }
warn() { printf '  \033[1;33mWARN\033[0m %s\n' "$*"; }   # non-fatal (packaged-runtime behavior is environment-sensitive)
log()  { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

# Enterprise footprints that must be absent from the community jar. The enterprise *code* cannot be in
# this repository any more, so the class markers guard against a vendored copy or a stray dependency;
# the library markers are the ones the enforcer gate bans.
ENTERPRISE_CLASSES=(
  io/ignifyr/runtime/streaming io/ignifyr/runtime/scheduling
  io/ignifyr/connector/kafka io/ignifyr/connector/fhirserver
  io/ignifyr/redcap io/ignifyr/format/delta io/ignifyr/format/json io/ignifyr/observability
  io/ignifyr/sink/omop io/ignifyr/server/
)
ENTERPRISE_LIBS=( org/apache/spark/sql/kafka io/delta/ it/sauronsoftware/cron4j net/logstash/logback )
# Every enterprise extension id prefix; none may appear in the community SPI manifest.
ENTERPRISE_EXT=(
  io.ignifyr.runtime.streaming io.ignifyr.runtime.scheduling
  io.ignifyr.connector.kafka io.ignifyr.connector.fhirserver io.ignifyr.redcap io.ignifyr.sink.omop
  io.ignifyr.format.delta
)
# Community distribution now includes the split-out FHIR + file sinks (sink-fhir / sink-file).
COMMUNITY_EXT=( io.ignifyr.connector.sql io.ignifyr.connector.file io.ignifyr.sink.fhir io.ignifyr.sink.file )

log "(Re)building the community fat jar (always fresh — a stale jar gives false results)"
( cd "$REPO_ROOT" && mvn -q -DskipTests -pl ignifyr-cli -am package ) || { echo "build failed" >&2; exit 1; }
[ -f "$CLI_JAR" ] || { echo "jar missing after build" >&2; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "unzip required" >&2; exit 1; }

# ---- jar content (best-effort, non-fatal) ------------------------------------
log "Jar content: enterprise code/libs excluded from the community jar"
# The community fat jar is a zip64 archive with ~96k entries, which Info-ZIP cannot enumerate; the JDK
# `jar` tool and Python's zipfile can. Collect candidate `jar` binaries (PATH, then the running JVM's
# java.home, then $JAVA_HOME); cygpath/CRLF handling keeps it working under Git Bash.
u() { command -v cygpath >/dev/null 2>&1 && cygpath -u "$1" 2>/dev/null || echo "$1"; }
JAR_CANDS=()
p="$(command -v jar 2>/dev/null || true)"; [ -n "$p" ] && JAR_CANDS+=("$p")
JHOME="$(java -XshowSettings:properties -version 2>&1 | sed -n 's/.*java\.home = *//p' | head -1 | tr -d '\r')"
[ -n "$JHOME" ] && { jh="$(u "$JHOME")"; JAR_CANDS+=("$jh/bin/jar" "$jh/bin/jar.exe"); }
[ -n "${JAVA_HOME:-}" ] && { jh="$(u "$JAVA_HOME")"; JAR_CANDS+=("$jh/bin/jar" "$jh/bin/jar.exe"); }
echo "  (jar candidates: ${JAR_CANDS[*]:-none})"
# First lister that yields NON-EMPTY output wins (a tool that exits 0 but prints nothing must not stop
# the cascade). Native Windows tools (jar.exe, Windows python) get a Windows path; MSYS tools a POSIX one.
list_jar() {
  local out c py z posix="$1" nat="$1"
  command -v cygpath >/dev/null 2>&1 && nat="$(cygpath -m "$posix" 2>/dev/null || echo "$posix")"
  for c in "${JAR_CANDS[@]:-}"; do
    [ -n "$c" ] || continue
    out="$("$c" tf "$nat" 2>/dev/null)"; [ -n "$out" ] && { printf '%s\n' "$out"; return 0; }
  done
  for py in python3 python; do
    command -v "$py" >/dev/null 2>&1 || continue
    out="$("$py" -c 'import zipfile,sys; [print(n) for n in zipfile.ZipFile(sys.argv[1]).namelist()]' "$nat" 2>/dev/null)"; [ -n "$out" ] && { printf '%s\n' "$out"; return 0; }
  done
  for z in "zipinfo -1" "unzip -Z1" "unzip -l"; do
    out="$($z "$posix" 2>/dev/null)"; [ -n "$out" ] && { printf '%s\n' "$out"; return 0; }
  done
  return 1
}
CLI_ENTRIES="$(list_jar "$CLI_JAR")"
# Match with bash's own substring test, NOT `echo "$VAR" | grep -q`. These listings are ~6 MB, and
# `grep -q` exits at the first match and closes the pipe, so `echo` dies of SIGPIPE (141); under
# `pipefail` (set at the top) that makes a *successful* match evaluate as false. Every check below
# inverted because of it: the guard reported "no jar lister could enumerate" against a jar that `jar
# tf` lists fine, so this whole section was silently skipped. Worse, had it run, a marker that IS
# present in the community jar would also have short-circuited grep and been reported as "excludes" —
# i.e. a real enterprise leak would have passed. No subprocess, no pipe, no SIGPIPE.
has() { [[ "$1" == *"$2"* ]]; }
# Skip (non-fatal) unless a lister actually enumerated each jar.
guard_ok=1
has "$CLI_ENTRIES" "io/ignifyr/engine/" || { warn "no jar lister could enumerate the community jar here — skipping this best-effort section (the SPI manifest + enforcer gate are authoritative)"; guard_ok=0; }
if [ "$guard_ok" -eq 0 ]; then
  echo "  (skipping jar-content checks — non-fatal; the ServiceLoader manifest and behavior checks below still run)"
else
for marker in "${ENTERPRISE_CLASSES[@]}" "${ENTERPRISE_LIBS[@]}"; do
  if has "$CLI_ENTRIES" "$marker"; then bad "community jar unexpectedly contains '$marker'"; else ok "community jar excludes '$marker'"; fi
done
fi

# ---- SPI manifest ------------------------------------------------------------
log "ServiceLoader manifest (io.ignifyr.engine.spi.IgnifyrExtension)"
CLI_SPI="$(unzip -p "$CLI_JAR" META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension 2>/dev/null)"
for ext in "${COMMUNITY_EXT[@]}"; do
  echo "$CLI_SPI" | grep -q "$ext" && ok "community SPI lists '$ext'" || bad "community SPI missing '$ext'"
done
for ext in "${ENTERPRISE_EXT[@]}"; do
  echo "$CLI_SPI" | grep -q "$ext" && bad "community SPI unexpectedly lists '$ext'" || ok "community SPI excludes '$ext'"
done

# ---- community CLI runtime behavior -----------------------------------------
log "Community CLI refuses enterprise jobs with an actionable error"
if command -v java >/dev/null 2>&1; then
  WS="$(mktemp -d)"; mkdir -p "$WS/mappings" "$WS/schemas"
  cp "$REPO_ROOT/ignifyr-testkit/src/main/resources/test-mappings/some-folder-1/patient-mapping.json" "$WS/mappings/"
  cp "$REPO_ROOT/ignifyr-testkit/src/main/resources/test-schemas/some-folder-1/Ext-patient.StructureDefinition.json" "$WS/schemas/"
  # The community CLI builds a SparkSession before it can report a missing capability; on JDK 17+ that
  # needs Spark's module opens (harmless on JDK 11). Give them to the forked JVM.
  JAVA_MAJOR="$(java -version 2>&1 | awk -F'"' '/version/{print $2; exit}' | awk -F'[._]' '{print ($1==1?$2:$1)}')"
  SPARK_OPENS=()
  if [ "${JAVA_MAJOR:-0}" -ge 17 ]; then
    echo "  (JDK $JAVA_MAJOR detected — adding Spark --add-opens for the forked CLI JVM)"
    SPARK_OPENS=(
      --add-opens=java.base/java.lang=ALL-UNNAMED
      --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
      --add-opens=java.base/java.io=ALL-UNNAMED
      --add-opens=java.base/java.net=ALL-UNNAMED
      --add-opens=java.base/java.nio=ALL-UNNAMED
      --add-opens=java.base/java.util=ALL-UNNAMED
      --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
      --add-opens=java.base/sun.security.action=ALL-UNNAMED
    )
  fi
  # Run an enterprise job on the community CLI and classify the outcome:
  #   names the missing capability/connector -> PASS
  #   timeout (rc 124: the CLI actually started the job) -> FAIL (leak)
  #   Spark/env could not start -> WARN
  #   ran but said nothing -> FAIL
  expect_refusal() { # <job-file> <grep-pattern> <label>
    local out rc
    out="$(timeout 300 java "${SPARK_OPENS[@]}" \
                            -Dignifyr.mappings.repository.folder-path="$WS/mappings" \
                            -Dignifyr.mappings.schemas.repository.folder-path="$WS/schemas" \
                            -jar "$CLI_JAR" run --job "$1" 2>&1)"; rc=$?
    if echo "$out" | grep -q "$2"; then
      ok "$3"
    elif [ "$rc" -eq 124 ]; then
      bad "$3 — CLI did NOT refuse the job; it started running it (enterprise capability leaked)"
    elif echo "$out" | grep -qiE 'winutils|HADOOP_HOME|UnsatisfiedLinkError|NativeIO|failed to create a child event loop|Unable to establish loopback|IllegalAccessError|does not export|add-opens'; then
      warn "$3 — could not evaluate (Spark environment failed to start; the registry is authoritatively checked by CommunityEditionSeparationSpec)"
    else
      bad "$3 — CLI ran but never reported the missing capability/connector"
    fi
  }
  expect_refusal "$SCRIPT_DIR/config/jobs/streaming-watch-job.json" "MissingCapabilityException\|runtime-streaming" "streaming job -> MissingCapabilityException (streaming)"
  # The Kafka source models live in the Enterprise connector-kafka module, so the community jar refuses
  # the job already when parsing it, naming the unknown jsonClass.
  expect_refusal "$SCRIPT_DIR/config/jobs/kafka-redcap-job.json"   "Unknown jsonClass 'KafkaSource" "Kafka streaming job -> refused (unknown Kafka source type)"
  # list-plugins must not advertise enterprise plugins on the community jar. Confirm it actually ran
  # first: with the rc and stderr dropped, a JVM that never got as far as printing anything left an
  # empty $lp, the grep matched nothing, and "no enterprise plugins" was reported -- the same verdict a
  # genuine leak would have produced. Keep stderr and require the header render() always prints.
  lp="$(java "${SPARK_OPENS[@]}" -jar "$CLI_JAR" list-plugins 2>&1)"
  if ! has "$lp" "Installed Ignifyr extension(s):"; then
    bad "list-plugins produced no usable output on the community jar (the JVM did not get as far as rendering)"
  elif echo "$lp" | grep -qi "runtime-streaming\|connector-kafka\|redcap"; then
    bad "list-plugins advertises an enterprise plugin on the community jar"
  else
    ok "list-plugins shows no enterprise plugins on the community jar"
  fi
  rm -rf "$WS"
else
  echo "  (java not on PATH; skipping runtime behavior checks — jar-content + SPI checks still ran)"
fi

# ---- summary -----------------------------------------------------------------
log "Summary"; printf '  passed: %s   failed: %s\n' "$PASS" "$FAILC"
[ "$FAILC" -eq 0 ]
