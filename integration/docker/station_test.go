//go:build docker
// +build docker

package docker_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMain brings up a service station in front of the cluster and points the
// suite at it.
//
// Why this suite's shape changed: it used to dial node ports directly. There is
// no such path in a deployment — the station is the only public entry point
// (§9.3(5)), and everything the station does is invisible to a client that
// connects straight to a node:
//
//   - routing a query to the storage layer (dial a control node directly and you
//     get "unknown service stratum.QueryService", which the retry rule treats as
//     terminal — a bug this suite could not see until the topology was split);
//   - refusing a replica whose cursor is behind, instead of returning a
//     complete-but-stale result;
//   - authenticating the caller, and stamping the internal mark that a node
//     configured with require_authenticated demands.
//
// The station runs on the host, dialing the node ports the cluster publishes;
// the suite dials the station. That is the deployed shape with the container
// boundary shifted by one hop.
//
// The cluster itself must already be up (scripts/cluster.sh --topology two-tier up).
// Set STRATUM_T4_STATION_ADDR to use an existing station instead of starting
// one — CI may want the station it deployed rather than a second one.
func TestMain(m *testing.M) {
	station, cleanup, err := startStation()
	if err != nil {
		fmt.Fprintf(os.Stderr, "docker_test: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	// nodeServices / storageServices keep the CONTAINER names. They are not
	// client entry points: they are what the fault-injection helpers kill and
	// start, and a kill has to name a container however the client reaches the
	// cluster.
	//
	// The defaults match scripts/cluster.sh --topology two-tier, whose containers carry
	// their tier in the name. Leaving the all-in-one names would make every kill
	// fail with "no such container" — which is exactly what happened: the suite
	// reached the cluster through the station and then could not fault it.
	nodeServices = splitEnv("STRATUM_T4_NODE_SERVICES",
		"stratum-node-control1,stratum-node-control2,stratum-node-control3")
	storageServices = splitEnv("STRATUM_T4_STORAGE_SERVICES",
		"stratum-node-storage1,stratum-node-storage2,stratum-node-storage3")

	// Every client-side address now points at the station. The lists stay
	// distinct in the tests' vocabulary because they mean different things —
	// where metadata lives versus where data lives — but telling them apart is
	// exactly what the station is for.
	//
	// The LENGTHS are not symmetric, and getting that wrong is a panic rather
	// than a wrong answer: nodeAddrs is indexed by control-node ordinal (the
	// leader index, the follower after it), while storageAddrs is indexed by
	// STORAGE-node ordinal everywhere it is used — `storageAddrs[i]` paired with
	// `storageServices[i]`, the rotation in the lag-catchup cases, the cold-read
	// pick in the latency case. Hard-coding three of each worked only while the
	// suite ran against three storage nodes; on a five-storage topology it
	// panicked with "index out of range [3] with length 3". So the storage list is
	// sized from the storage tier, and every entry is the station: the station is
	// the tier's read entry point, and which node answers is its decision.
	nodeAddrs = make([]string, len(nodeServices))
	storageAddrs = make([]string, len(storageReplicaAddrs))
	for i := range nodeAddrs {
		nodeAddrs[i] = station
	}
	for i := range storageAddrs {
		storageAddrs[i] = station
	}
	if len(storageServices) > len(storageAddrs) {
		fmt.Fprintf(os.Stderr,
			"docker_test: %d storage containers but %d storage addresses; "+
				"set STRATUM_T4_STORAGE_NODE_ADDRS to match STRATUM_T4_STORAGE_SERVICES\n",
			len(storageServices), len(storageAddrs))
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// startStation returns the station's address, or an existing one when
// STRATUM_T4_STATION_ADDR names it.
//
// STRATUM_T4_STATION_NETWORK runs the station INSIDE that docker network
// instead of on the host (see startStationInNetwork). It is required for any
// suite that exercises per-KB placement, because the station now routes by the
// addresses the DIRECTORY holds — and a two-tier deployment registers its
// storage nodes by container name, which a host process cannot resolve.
func startStation() (addr string, cleanup func(), err error) {
	if existing := os.Getenv("STRATUM_T4_STATION_ADDR"); existing != "" {
		return existing, func() {}, nil
	}
	if network := os.Getenv("STRATUM_T4_STATION_NETWORK"); network != "" {
		return startStationInNetwork(network)
	}

	// controlAddrs is the package-level list await_direct_test.go declares — the
	// nodes themselves, not the station. The station dials exactly the addresses
	// the fault-injection helpers reason about, and one variable is what keeps the
	// two from drifting apart.
	storageNodeAddrs := splitEnv("STRATUM_T4_STORAGE_NODE_ADDRS", "localhost:17100,localhost:17101,localhost:17102")

	// Pick a free port for the station so a locally running one does not clash.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, fmt.Errorf("pick a station port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	addr = fmt.Sprintf("127.0.0.1:%d", port)

	bin, err := buildRouter()
	if err != nil {
		return "", nil, err
	}

	args := []string{
		"-listen", addr,
		"-nodes", joinAddrs(controlAddrs),
		"-storage-nodes", joinAddrs(storageNodeAddrs),
	}
	// The nodes require the station's trust mark (scripts/cluster.sh writes
	// require_authenticated: true and a station_secret into every node config),
	// and since H4 that mark is an HMAC only the holder of the key can produce.
	// A station started without the key forwards unmarked calls, which every node
	// refuses — so the station has to be given the same secret the nodes check.
	if secret := stationSecret(); len(secret) > 0 {
		args = append(args, "-station-secret", string(secret))
	}
	// The station writes to its own file rather than to this process's stderr.
	//
	// That is not tidiness: go test waits for the stdout/stderr it handed the
	// test binary to be closed, and a background child still holding them makes
	// the run report "Test I/O incomplete ... WaitDelay expired" — a FAIL with
	// every test marked PASS, which is exactly what happened here. Pointing the
	// child at a file also means its log is somewhere an operator can read it
	// when a test fails.
	logPath := filepath.Join(os.TempDir(), "stratum-router-test.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return "", nil, fmt.Errorf("create the station's log: %w", err)
	}

	cmd := exec.Command(bin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return "", nil, fmt.Errorf("start station: %w", err)
	}
	fmt.Fprintf(os.Stderr, "docker_test: station on %s, log: %s\n", addr, logPath)

	cleanup = func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
		_ = os.Remove(bin)
	}

	if err := waitForListen(addr, 10*time.Second); err != nil {
		cleanup()
		return "", nil, err
	}
	return addr, cleanup, nil
}

// startStationInNetwork runs the station as a container ON the cluster's docker
// network and returns the host port that reaches it.
//
// Why a container: the per-KB placement work made the directory the routing
// authority, and the directory holds the addresses the deployment registered —
// "stratum-node-storage1:7000" in a two-tier cluster. Those names resolve only
// inside the network, so a host-side station answered every query with "storage
// unavailable: all nodes unavailable" (measured on the five-storage-node
// cluster). Running the station beside the nodes is also the shape the plan
// describes (docs/实施中/storage-topology-plan.md §3): the station is part of the
// cluster, not a client outside it.
//
// The binary is built statically (CGO_ENABLED=0) and mounted from a directory
// under run/ rather than /tmp: this environment's docker cannot mount /tmp, and
// run/ is already ignored by git.
func startStationInNetwork(network string) (string, func(), error) {
	root, err := repoRoot()
	if err != nil {
		return "", nil, err
	}
	runDir := filepath.Join(root, "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("create run dir: %w", err)
	}
	dir, err := os.MkdirTemp(runDir, "station-net-")
	if err != nil {
		return "", nil, fmt.Errorf("temp dir for the station binary: %w", err)
	}
	bin := filepath.Join(dir, "stratum-router")

	build := exec.Command("go", "build", "-o", bin, "./cmd/stratum-router/")
	build.Dir = root
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("build the station: %w", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("pick a station port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	image := os.Getenv("STRATUM_T4_STATION_IMAGE")
	if image == "" {
		image = "alpine:latest"
	}
	name := fmt.Sprintf("stratum-test-station-%d", os.Getpid())

	// The nodes are named, not addressed: inside the network their container
	// names are the addresses the directory holds, and every node listens on
	// 7000 there (scripts/cluster.sh publishes host ports, not container ones).
	inNetwork := func(names []string) string {
		out := make([]string, 0, len(names))
		for _, n := range names {
			out = append(out, n+":7000")
		}
		return strings.Join(out, ",")
	}
	controls := splitEnv("STRATUM_T4_NODE_SERVICES",
		"stratum-node-control1,stratum-node-control2,stratum-node-control3")
	storages := splitEnv("STRATUM_T4_STORAGE_SERVICES",
		"stratum-node-storage1,stratum-node-storage2,stratum-node-storage3")

	args := []string{
		"run", "-d", "--rm", "--name", name,
		"--network", network,
		"-p", fmt.Sprintf("127.0.0.1:%d:7000", port),
		"-v", dir + ":/app:ro",
		"-w", "/app",
		image, "/app/stratum-router",
		"-listen", "0.0.0.0:7000",
		"-nodes", inNetwork(controls),
		"-storage-nodes", inNetwork(storages),
	}
	if secret := stationSecret(); len(secret) > 0 {
		args = append(args, "-station-secret", string(secret))
	}
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("start the station container: %w\n%s", err, out)
	}

	cleanup := func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
		_ = os.RemoveAll(dir)
	}
	if err := waitForListen(addr, 30*time.Second); err != nil {
		logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
		cleanup()
		return "", nil, fmt.Errorf("%w\nstation logs:\n%s", err, logs)
	}
	fmt.Fprintf(os.Stderr, "docker_test: station container %s on %s (network %s)\n", name, addr, network)
	return addr, cleanup, nil
}

// buildRouter compiles cmd/stratum-router into a temp file.
//
// Built here rather than assumed on PATH: a stale binary from an earlier run
// would silently test the wrong station, and "which build am I testing" is not a
// question a test suite should leave open.
func buildRouter() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp("", "stratum-router-")
	if err != nil {
		return "", err
	}
	bin := filepath.Join(dir, "stratum-router")

	build := exec.Command("go", "build", "-o", bin, "./cmd/stratum-router/")
	build.Dir = root
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("build the station: %w", err)
	}
	return bin, nil
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

func joinAddrs(addrs []string) string {
	out := ""
	for i, a := range addrs {
		if i > 0 {
			out += ","
		}
		out += a
	}
	return out
}

// waitForListen blocks until the station accepts connections, so tests never
// race its startup.
func waitForListen(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("station at %s did not come up within %v", addr, timeout)
}
