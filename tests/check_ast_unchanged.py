"""Chứng minh một thay đổi CHỈ thêm/sửa comment và docstring: so AST (đã bỏ docstring) của file ở một
git ref với file hiện tại. Comment không có trong AST; docstring bị loại trước khi so.

    python tests/check_ast_unchanged.py <git-ref> <file.py> [<file.py> ...]
Exit 0 = mọi file có AST giống hệt (logic không đổi), 1 = có file khác.
"""
import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def strip_docstrings(tree):
    """Bỏ chuỗi docstring đứng đầu module / class / function để chỉ so phần code thực thi."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], 'value', None), ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return tree


def normalized(source):
    return ast.dump(strip_docstrings(ast.parse(source)), include_attributes=False)


def main():
    ref, files = sys.argv[1], sys.argv[2:]
    bad = 0
    for name in files:
        path = Path(name)
        rel = path.resolve().relative_to(ROOT).as_posix()
        old = subprocess.run(['git', 'show', f'{ref}:{rel}'], cwd=ROOT, capture_output=True, text=True,
                             encoding='utf-8', check=True).stdout
        new = path.read_text(encoding='utf-8')
        same = normalized(old) == normalized(new)
        bad += not same
        print(f'[{"PASS" if same else "FAIL"}] {rel}: AST (không tính docstring) '
              f'{"giống hệt" if same else "KHÁC"} {ref}')
    print(f'RESULT: {"PASS" if not bad else "FAIL"} ({len(files) - bad}/{len(files)} files)')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
