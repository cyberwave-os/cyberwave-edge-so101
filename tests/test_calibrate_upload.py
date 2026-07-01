"""Source-level tests for calibration upload behavior in cw_calibrate.py."""

from __future__ import annotations

import ast
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _find_main_function(tree: ast.AST) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return node
    raise AssertionError("main() not found in scripts/cw_calibrate.py")


def _is_save_calibration_stmt(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
        return False
    func = stmt.value.func
    return isinstance(func, ast.Attribute) and func.attr == "save_calibration"


def _is_upload_stmt(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
        return False
    func = stmt.value.func
    return (
        isinstance(func, ast.Name)
        and func.id == "_upload_calibration_to_twin_if_available"
    )


def _assert_branch_saves_then_uploads(statements: list[ast.stmt]) -> None:
    save_idx = next((i for i, s in enumerate(statements) if _is_save_calibration_stmt(s)), None)
    upload_idx = next((i for i, s in enumerate(statements) if _is_upload_stmt(s)), None)
    assert save_idx is not None, "Expected device.save_calibration() in branch"
    assert upload_idx is not None, "Expected immediate upload helper call in branch"
    assert upload_idx > save_idx, "Upload helper must run after save_calibration()"

    upload_stmt = statements[upload_idx]
    assert isinstance(upload_stmt, ast.Expr) and isinstance(upload_stmt.value, ast.Call)
    upload_call = upload_stmt.value
    assert len(upload_call.args) == 3, "Upload helper call should pass device, robot, and robot_type"
    robot_type_arg = upload_call.args[2]
    assert isinstance(robot_type_arg, ast.Attribute)
    assert isinstance(robot_type_arg.value, ast.Name)
    assert robot_type_arg.value.id == "args"
    assert robot_type_arg.attr == "type"


def test_cw_calibrate_uploads_to_twin_after_local_save_in_both_paths():
    """Both leader/recalibration and follower-first-calibration branches upload immediately."""
    source_path = _project_root() / "scripts" / "cw_calibrate.py"
    source = source_path.read_text()
    tree = ast.parse(source)
    main_fn = _find_main_function(tree)

    calibrate_if = None
    for node in ast.walk(main_fn):
        if not isinstance(node, ast.If):
            continue
        has_save_in_body = any(_is_save_calibration_stmt(stmt) for stmt in node.body)
        has_save_in_else = any(_is_save_calibration_stmt(stmt) for stmt in node.orelse)
        if has_save_in_body and has_save_in_else:
            calibrate_if = node
            break

    assert calibrate_if is not None, "Expected leader/follower branch in main()"
    _assert_branch_saves_then_uploads(calibrate_if.body)
    _assert_branch_saves_then_uploads(calibrate_if.orelse)
