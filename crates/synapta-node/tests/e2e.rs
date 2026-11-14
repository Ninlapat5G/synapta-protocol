//! End-to-end: three real `synapta-node` processes on loopback, driven by `synapta-sandbox`.
//! The launcher checks every scenario step itself; this test checks it passed and that the
//! evidence it leaves behind (transcript, separate per-node logs) is there.

use std::process::Command;

use serde_json::Value;

#[test]
fn sandbox_scenario_passes_with_three_processes() {
    let dir = tempfile::tempdir().unwrap();
    let sandbox_dir = dir.path().join("run");
    let output = Command::new(env!("CARGO_BIN_EXE_synapta-sandbox"))
        .arg("--auto-ports")
        .arg("--dir")
        .arg(&sandbox_dir)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "sandbox failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for step in [
        "TEST-01",
        "TEST-02",
        "TEST-03",
        "TEST-04",
        "TEST-05",
        "TEST-06",
        "TEST-07",
        "TEST-08",
        "TEST-10",
        "graceful shutdown",
    ] {
        assert!(
            stdout.lines().any(|l| l.starts_with("PASS") && l.contains(step)),
            "{step} not passed:\n{stdout}"
        );
    }

    let transcript: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(sandbox_dir.join("transcript.json")).unwrap()).unwrap();
    let math = transcript
        .iter()
        .find(|t| t["path"] == "/tools/math.add" && t["status"] == 200)
        .unwrap();
    assert_eq!(math["response"]["executed_by"], "node-b");

    // Separate processes → separate configs, data dirs and log files.
    for node in ["node-a", "node-b", "node-c"] {
        let log = std::fs::read_to_string(sandbox_dir.join("data").join(node).join("node.log")).unwrap();
        assert!(log.lines().count() > 3, "{node} wrote its own log");
        assert!(
            log.lines().all(|l| serde_json::from_str::<Value>(l).is_ok()),
            "{node} log is JSON lines"
        );
        // Regression: a node's own advert relayed back once raised a false "untrusted" warning.
        assert!(
            !log.contains("not signed by its trusted key"),
            "{node} logged a false trust alarm"
        );
    }
    // Gracefully stopped nodes remove their runtime file; the killed node-b could not.
    assert!(!sandbox_dir.join("data/node-a/runtime.json").exists());
    assert!(sandbox_dir.join("data/node-b/runtime.json").exists());
}
