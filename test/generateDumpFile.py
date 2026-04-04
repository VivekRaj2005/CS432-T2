import argparse
import json
import random
from pathlib import Path

from faker import Faker


def parse_args():
	parser = argparse.ArgumentParser(
		description="Generate a dummy JSON dump file using Faker/random"
	)
	parser.add_argument(
		"-c",
		"--count",
		type=int,
		default=100,
		help="Number of records to generate (default: 100)",
	)
	parser.add_argument(
		"-o",
		"--output",
		default="dummy_dump.json",
		help="Output dump file path (default: dummy_dump.json)",
	)
	parser.add_argument(
		"--seed",
		type=int,
		default=432,
		help="Seed for deterministic Faker/random output (default: 432)",
	)
	parser.add_argument(
		"--clubs-null-rate",
		type=float,
		default=0.30,
		help="Probability of Clubs being NULL (default: 0.30)",
	)
	return parser.parse_args()


def make_course_codes() -> list[str]:
	prefixes = ["CS", "MA", "EE", "ME", "PH", "HS"]
	count = random.randint(3, 6)
	codes = {
		f"{random.choice(prefixes)}{random.randint(100, 499)}"
		for _ in range(count)
	}
	return sorted(codes)


def make_record(fake: Faker, clubs_null_rate: float) -> dict:
	grades = ["A", "A-", "B+", "B", "B-", "C+", "C", "D", "F"]

	clubs_value = None
	if random.random() > clubs_null_rate:
		clubs_value = random.choice([
			"Robotics",
			"Drama",
			"Music",
			"Coding",
			"Photography",
			"Literary",
			"Debate",
		])

	return {
		"username": fake.unique.user_name(),
		"grade": random.choice(grades),
		"marks": random.randint(0, 100),
		"prof": {
			"name": fake.name(),
			"dept": random.choice([
				"CSE",
				"EEE",
				"ME",
				"Physics",
				"Mathematics",
				"Humanities",
			]),
		},
		"Course Codes": make_course_codes(),
		"Clubs": clubs_value,
	}


def main():
	args = parse_args()
	if args.count <= 0:
		raise ValueError("--count must be greater than 0")
	if not (0.0 <= args.clubs_null_rate <= 1.0):
		raise ValueError("--clubs-null-rate must be between 0 and 1")

	random.seed(args.seed)
	fake = Faker()
	fake.seed_instance(args.seed)

	records = [make_record(fake, args.clubs_null_rate) for _ in range(args.count)]

	dump = {
		"schema_version": 1,
		"record_count": len(records),
		"data": records,
	}

	output_path = Path(args.output)
	output_path.parent.mkdir(parents=True, exist_ok=True)
	output_path.write_text(json.dumps(dump, indent=2, ensure_ascii=False), encoding="utf-8")

	print(f"Generated dummy dump with {len(records)} records at: {output_path}")


if __name__ == "__main__":
	main()
