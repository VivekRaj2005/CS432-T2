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
	parser.add_argument(
		"--typo-rate",
		type=float,
		default=0,
		help="Probability of generating a typo-heavy record (default: 0)",
	)
	parser.add_argument(
		"--field-typo-rate",
		type=float,
		default=0.01,
		help="Probability of corrupting a field name inside a typo-heavy record (default: 0.01)",
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


def typo_field_name(field_name: str) -> str:
	typo_map = {
		"username": "usrename",
		"grade": "garde",
		"marks": "mraks",
		"prof": "prfo",
		"Course Codes": "Course Cdoes",
		"Clubs": "Clbubs",
	}
	if field_name in typo_map:
		return typo_map[field_name]
	if len(field_name) > 4:
		chars = list(field_name)
		i = random.randint(0, len(chars) - 2)
		chars[i], chars[i + 1] = chars[i + 1], chars[i]
		return "".join(chars)
	return field_name


def maybe_typo_record(record: dict, typo_rate: float, field_typo_rate: float) -> dict:
	if random.random() > typo_rate:
		return record

	typo_record: dict = {}
	for key, value in record.items():
		new_key = typo_field_name(key) if random.random() < field_typo_rate else key

		if key == "username" and isinstance(value, str):
			value = typo_field_name(value) if random.random() < 0.5 else value[::-1]
		elif key == "grade" and isinstance(value, str):
			value = value.lower() if random.random() < 0.5 else f"{value}?"
		elif key == "marks" and isinstance(value, int):
			value = value * random.choice([-1, 1]) if random.random() < 0.5 else value + random.randint(100, 300)
		elif key == "prof" and isinstance(value, dict):
			value = {
				(typo_field_name("name") if random.random() < field_typo_rate else "name"): value.get("name"),
				(typo_field_name("dept") if random.random() < field_typo_rate else "dept"): value.get("dept"),
			}
		elif key == "Course Codes" and isinstance(value, list):
			value = value + [f"XX{random.randint(900, 999)}"] if random.random() < 0.5 else value[: max(1, len(value) - 1)]
		elif key == "Clubs" and random.random() < 0.5:
			value = None

		typo_record[new_key] = value

	return typo_record


def make_record(fake: Faker, clubs_null_rate: float, typo_rate: float, field_typo_rate: float) -> dict:
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

	record = {
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

	return maybe_typo_record(record, typo_rate, field_typo_rate)


def main():
	args = parse_args()
	if args.count <= 0:
		raise ValueError("--count must be greater than 0")
	if not (0.0 <= args.clubs_null_rate <= 1.0):
		raise ValueError("--clubs-null-rate must be between 0 and 1")
	if not (0.0 <= args.typo_rate <= 1.0):
		raise ValueError("--typo-rate must be between 0 and 1")
	if not (0.0 <= args.field_typo_rate <= 1.0):
		raise ValueError("--field-typo-rate must be between 0 and 1")

	random.seed(args.seed)
	fake = Faker()
	fake.seed_instance(args.seed)

	records = [
		make_record(fake, args.clubs_null_rate, args.typo_rate, args.field_typo_rate)
		for _ in range(args.count)
	]

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
