import argparse
from pathlib import Path
import py_compile
from os.path import join
import asyncio
import json
from collections import deque

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
	parser.add_argument(
		"-q",
		"--queue-file",
		help="Path to JSON/JSONL file containing commands for MapRegister queue test"
	)
	parser.add_argument(
		"--update-log",
		default="update.log",
		help="Path to write updateOrder logs (default: update.log)"
	)
	return parser.parse_args()


def _load_command_queue(queue_file: Path) -> deque:
	if not queue_file.exists():
		raise FileNotFoundError(f"Queue file not found: {queue_file}")
	raw = queue_file.read_text(encoding="utf-8").strip()
	if not raw:
		return deque()

	commands = None
	# First, try full JSON (list/dict).
	try:
		parsed = json.loads(raw)
		if isinstance(parsed, list):
			commands = parsed
		elif isinstance(parsed, dict):
			commands = [parsed]
	except json.JSONDecodeError:
		commands = None

	# Fallback: JSONL format (one JSON object per line).
	if commands is None:
		commands = []
		for line in raw.splitlines():
			line = line.strip()
			if not line:
				continue
			commands.append(json.loads(line))

	return deque(commands)


def _normalize_command_to_payload(command: dict) -> dict:
	# Supports either direct payloads or simulator-style envelopes: {"event": ..., "data": "{...}"}
	if isinstance(command, dict) and "data" in command:
		data = command["data"]
		if isinstance(data, str):
			return json.loads(data)
		if isinstance(data, dict):
			return data
	if isinstance(command, dict):
		return command
	raise ValueError(f"Unsupported command type: {type(command)}")


def test_mapregister_queue(command_queue: deque, update_log_path: Path = Path("update.log"), save_every: int = 1000):
	from utils.mapregister import MapRegister

	mr = MapRegister(table_name="root")
	update_log_path.write_text("", encoding="utf-8")

	processed = 0
	with update_log_path.open("a", encoding="utf-8") as log_file:
		while command_queue:
			command = command_queue.popleft()
			payload = _normalize_command_to_payload(command)
			update_order = deque()
			mr.ResolveRequest(payload, updateOrder=update_order)

			processed += 1
			entry = {
				"request_index": processed,
				"request": payload,
				"updateOrder": list(update_order)
			}
			log_file.write(json.dumps(entry, default=str) + "\n")

			if processed % save_every == 0:
				state_file = Path("map_register.pkl")
				mr.Save(str(state_file))
				log_file.write(json.dumps({
					"checkpoint": str(state_file),
					"processed": processed
				}) + "\n")

	print(f"Processed {processed} commands. Update order written to {update_log_path}")
	return mr


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
	if target.name == "mapregister.py":
		if not args.queue_file:
			raise ValueError("--queue-file is required when testing mapregister.py")
		queue_path = Path(args.queue_file)
		command_queue = _load_command_queue(queue_path)
		test_mapregister_queue(command_queue, update_log_path=Path(args.update_log), save_every=1000)


if __name__ == "__main__":
	main()

