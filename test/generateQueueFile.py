import argparse
import json
import random
from pathlib import Path


def parse_args():
	parser = argparse.ArgumentParser(
		description="Generate a queue file for test.py mapregister queue testing"
	)
	parser.add_argument(
		"-c",
		"--count",
		type=int,
		default=1200,
		help="Number of commands to generate (default: 1200)",
	)
	parser.add_argument(
		"-o",
		"--output",
		default="sample_queue.json",
		help="Output queue file path (default: sample_queue.json)",
	)
	parser.add_argument(
		"-f",
		"--format",
		choices=["json", "jsonl"],
		default="json",
		help="Output format: json or jsonl (default: json)",
	)
	parser.add_argument(
		"--seed",
		type=int,
		default=432,
		help="Random seed for deterministic generation (default: 432)",
	)
	parser.add_argument(
		"--envelope",
		action="store_true",
		help="Wrap each payload as {event, data} where data is JSON string",
	)
	return parser.parse_args()


def make_payload(index):
	# Rotate payload shapes to trigger type resolution, ALTER, and storage-path behavior.
	mode = index % 10
	if mode == 0:
		return {
			"record_id": index,
			"credits": random.randint(1, 5),
			"active": True,
			"name": f"student_{index}",
		}
	if mode == 1:
		return {
			"record_id": index,
			"credits": [1, 2, 3],
			"active": "false",
			"name": f"student_{index}",
		}
	if mode == 2:
		return {
			"record_id": index,
			"credits": [1.5, 2.5, 3.5],
			"active": 1,
			"profile": {"level": "UG", "year": 2026},
		}
	if mode == 3:
		return {
			"record_id": f"ID-{index}",
			"credits": ["1", "2", "3"],
			"profile": {"level": "PG", "year": 2027},
		}
	if mode == 4:
		return {
			"record_id": index,
			"credits": "4",
			"active": "yes",
			"tags": ["core", "elective"],
		}
	if mode == 5:
		return {
			"record_id": index,
			"credits": {"current": 3, "max": 5},
			"active": False,
			"meta": {"source": "generator", "batch": index // 10},
		}
	if mode == 6:
		return {
			"record_id": index,
			"credits": [True, False, True],
			"active": "0",
			"name": f"student_{index}",
		}
	if mode == 7:
		return {
			"record_id": index,
			"credits": ["3.0", "4.0"],
			"active": "1",
			"profile": {"level": "UG", "honors": index % 2 == 0},
		}
	if mode == 8:
		return {
			"record_id": index,
			"credits": random.uniform(1.0, 5.0),
			"active": random.choice([True, False]),
			"tags": [],
		}
	return {
		"record_id": index,
		"credits": random.choice([1, 2, 3, 4, 5]),
		"active": random.choice(["true", "false", "yes", "no", 0, 1]),
		"profile": {"level": random.choice(["UG", "PG"]), "year": random.randint(2024, 2028)},
	}


def wrap_command(payload, use_envelope):
	if not use_envelope:
		return payload
	return {
		"event": "add",
		"data": json.dumps(payload),
	}


def write_commands(commands, output_path, output_format):
	output_path.parent.mkdir(parents=True, exist_ok=True)
	if output_format == "json":
		output_path.write_text(json.dumps(commands, indent=2), encoding="utf-8")
		return
	with output_path.open("w", encoding="utf-8") as f:
		for cmd in commands:
			f.write(json.dumps(cmd) + "\n")


def main():
	args = parse_args()
	if args.count <= 0:
		raise ValueError("--count must be greater than 0")

	random.seed(args.seed)
	output_path = Path(args.output)

	commands = []
	for i in range(1, args.count + 1):
		payload = make_payload(i)
		commands.append(wrap_command(payload, args.envelope))

	write_commands(commands, output_path, args.format)

	print(f"Generated {args.count} commands at {output_path}")
	print(
		"Run with: python test.py -t utils/mapregister.py "
		f"-q {output_path} --update-log update.log"
	)


if __name__ == "__main__":
	main()
