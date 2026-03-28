import argparse
from pathlib import Path
import py_compile
from os.path import join
import asyncio

FILES = {
	"network.py": join("utils", "network.py"),
}

def parse_args():
	parser = argparse.ArgumentParser(description="Test a Python file")
	parser.add_argument(
		"-t",
		"--target",
		required=True,
		help="Path to the Python file to test"
	)
	return parser.parse_args()


def main():
	args = parse_args()
	if args.target in FILES:
		args.target = FILES[args.target]
	target = Path(args.target)

	if not target.exists():
		raise FileNotFoundError(f"Target file not found: {target}")
	if target.suffix != ".py":
		raise ValueError(f"Target must be a Python file: {target}")

	py_compile.compile(str(target), doraise=True)
	print(f"Syntax check passed: {target}")
	print(f"Running tests for: {target}")
	if target.name == "network.py":
		from utils.network import test
		asyncio.run(test())


if __name__ == "__main__":
	main()

