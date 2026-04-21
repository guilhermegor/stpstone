"""Docstring type/raises consistency checker for stpstone and tests packages."""

import ast
import pathlib
import re
import sys
from typing import Any, Dict, Set


def compare_types(hint: Any, doc: str) -> bool:
	if hint is Any or doc.lower() == "any":
		return True
	hint_str = str(hint).replace("typing.", "").lower()
	doc = doc.lower().strip()
	equivalences = {
		"list": "sequence",
		"dict": "mapping",
		"np.ndarray": "ndarray",
		"numpy.ndarray": "ndarray",
	}
	for k, v in equivalences.items():
		hint_str = hint_str.replace(k.lower(), v)
		doc = doc.replace(k.lower(), v)
	return hint_str == doc


def parse_raises_section(docstring: str) -> Dict[str, str]:
	raises: Dict[str, str] = {}
	if not docstring:
		return raises
	lines = [line.rstrip() for line in docstring.splitlines()]
	in_raises = False
	for line in lines:
		stripped = line.strip()
		if re.match(r"^(Raises|Raises:)$", stripped, re.IGNORECASE):
			in_raises = True
			continue
		if not in_raises:
			continue
		if not stripped:
			continue
		if re.match(
			r"^(Args|Arguments|Parameters|Returns|Yields|Notes|Examples|Attributes|See Also|References)(:)?$",
			stripped,
			re.IGNORECASE,
		):
			break
		match = re.match(r"^([\w.]+)\s*:\s*(.*)", stripped)
		if match:
			exc, desc = match.groups()
			raises[exc.strip()] = desc.strip()
		else:
			name_match = re.match(r"^([\w.]+)$", stripped)
			if name_match:
				raises[name_match.group(1)] = ""
	return raises


def get_actual_raises(node: ast.AST) -> Set[str]:
	raises: Set[str] = set()
	for n in ast.walk(node):
		if not isinstance(n, ast.Raise) or n.exc is None:
			continue
		if isinstance(n.exc, ast.Name):
			raises.add(n.exc.id)
		elif isinstance(n.exc, ast.Call) and isinstance(n.exc.func, ast.Name):
			raises.add(n.exc.func.id)
		elif isinstance(n.exc, ast.Attribute):
			raises.add(n.exc.attr)
		elif isinstance(n.exc, ast.Call) and hasattr(n.exc.func, "id"):
			raises.add(n.exc.func.id)
	return raises


def normalize_exception_name(name: str) -> str:
	return name.split(".")[-1]


_SECTION_RE = re.compile(
	r"^(Args|Arguments|Parameters|Returns|Yields|Notes|Examples|Attributes|See Also|References|Raises)(:)?$",
	re.IGNORECASE,
)


def check_file(filepath: str) -> int:
	errors = 0
	with open(filepath, "r", encoding="utf-8") as fh:
		tree = ast.parse(fh.read(), filename=filepath)
	for node in ast.walk(tree):
		if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
			continue
		lineno = node.lineno
		has_return_annotation = node.returns is not None
		is_property = any(
			isinstance(d, ast.Name) and d.id == "property" for d in node.decorator_list
		)
		if not has_return_annotation and not is_property:
			continue
		docstring = ast.get_docstring(node)
		if not docstring:
			print(f"⚠️  Missing docstring in {node.name}() at line {lineno} ({filepath})")
			errors += 1
			continue
		if has_return_annotation:
			returns = ast.unparse(node.returns)
			doc_lines = [l.rstrip() for l in docstring.split("\n")]
			has_returns_section = any(line.strip().lower() == "returns" for line in doc_lines)
			if has_returns_section:
				found_return = False
				for i, line in enumerate(doc_lines):
					if line.strip().lower() != "returns":
						continue
					j = i + 1
					while j < len(doc_lines) and set(doc_lines[j].strip()) <= {"-", " "}:
						j += 1
					while j < len(doc_lines):
						candidate = doc_lines[j].strip()
						if candidate:
							type_lines = [candidate]
							base_indent = len(doc_lines[j]) - len(doc_lines[j].lstrip())
							k = j + 1
							while k < len(doc_lines):
								nxt = doc_lines[k]
								nxt_strip = nxt.strip()
								if not nxt_strip:
									break
								nxt_indent = len(nxt) - len(nxt.lstrip())
								if nxt_indent > base_indent:
									break
								if _SECTION_RE.match(nxt_strip):
									break
								type_lines.append(nxt_strip)
								k += 1
							doc_type = " ".join(type_lines)
							if not compare_types(returns, doc_type):
								print(
									f"❌ Return type mismatch in {node.name}() at line {lineno} ({filepath}):"
								)
								print(f"   Type hint: {returns}")
								print(f"   Docstring: {doc_type}")
								errors += 1
							found_return = True
							break
						j += 1
					break
				if not found_return:
					print(
						f"⚠️  Return type documented but no type found in {node.name}() at line {lineno} ({filepath})"
					)
					errors += 1
		for arg in node.args.args:
			if arg.arg == "self" or not arg.annotation:
				continue
			hint = ast.unparse(arg.annotation)
			arg_doc_found = False
			param_lines = docstring.split("\n")
			for li, line in enumerate(param_lines):
				if arg.arg not in line or ":" not in line:
					continue
				type_part = line.split(":")[1].strip()
				line_indent = len(line) - len(line.lstrip())
				k = li + 1
				while k < len(param_lines):
					nxt = param_lines[k]
					nxt_strip = nxt.strip()
					if not nxt_strip:
						break
					nxt_indent = len(nxt) - len(nxt.lstrip())
					if nxt_indent > line_indent:
						break
					if re.match(r"^[\w.]+\s*:", nxt_strip):
						break
					if _SECTION_RE.match(nxt_strip):
						break
					type_part += " " + nxt_strip
					k += 1
				if not compare_types(hint, type_part):
					print(
						f"❌ Parameter type mismatch in {node.name}({arg.arg}) at line {lineno} ({filepath}):"
					)
					print(f"   Type hint: {hint}")
					print(f"   Docstring: {type_part}")
					errors += 1
				arg_doc_found = True
				break
			if not arg_doc_found:
				print(
					f"⚠️  Missing docstring for parameter {arg.arg} in {node.name}() at line {lineno} ({filepath})"
				)
				errors += 1
		doc_raises = parse_raises_section(docstring)
		actual_raises = get_actual_raises(node)
		doc_exceptions = {normalize_exception_name(e) for e in doc_raises}
		actual_exceptions = {normalize_exception_name(e) for e in actual_raises}
		for exc in doc_exceptions:
			if exc not in actual_exceptions:
				print(
					f"⚠️  Documented but not raised exception {exc} in {node.name}() at line {lineno} ({filepath})"
				)
				errors += 1
		for exc in actual_exceptions:
			if exc not in doc_exceptions:
				print(
					f"⚠️  Raised but not documented exception {exc} in {node.name}() at line {lineno} ({filepath})"
				)
				errors += 1
	return errors


if __name__ == "__main__":
	targets = list(pathlib.Path("stpstone").rglob("*.py")) + list(
		pathlib.Path("tests").rglob("*.py")
	)
	total_errors = sum(check_file(str(p)) for p in targets)
	sys.exit(1 if total_errors > 0 else 0)
