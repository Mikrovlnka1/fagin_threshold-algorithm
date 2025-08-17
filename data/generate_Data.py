#!/usr/bin/env python3


"""
generate_phones.py

Script for random generation of a mobile phone dataset and saving it to a CSV file.
Allows you to specify any number of rows (argument --count).
Each row contains:
    - model (string): brand + "Model" + unique number
    - display_freq (int): screen refresh rate in Hz (60 - 144)
    - battery (int): battery capacity (3000 - 6000 mAh)
    - price (int): price (3000 - 40000 CZK)
    - ram (int): RAM size (3 - 16 GB)
    - size (float): screen size (4.5 - 7.2 inches)
    - camera_res (float): camera resolution in Mpx (8.0 - 108.0)
"""


import argparse
import csv
import random

#list o brands
BRANDS = [
    "Samsung", "Apple", "Xiaomi", "OnePlus", "Huawei", "Motorola",
    "Realme", "Oppo", "Sony", "Asus", "Nokia", "Google"
]


def generate_phones(count):
    """
        Generates a list of dictionaries (of length 'count') with keys:
          - 'model' (string)
          - 'display_freq' (int, 60–144)
          - 'battery' (int, 3000–6000)
          - 'price' (int, 3000–40000)
          - 'ram' (int, 3–16)
          - 'size' (float, 4.5–7.2)
          - 'camera_res' (float, 8.0–108.0)
    """

    phones = []

    for i in range(count):
        # 1) Random selection of brand
        brand = random.choice(BRANDS)

        # 2) each element will have its unique id
        unique_id = i + 1

        # 3) model_name
        model_name = f"{brand} Model {unique_id}"

        # 4) display_freq - integer between 60 and 144
        display_freq = random.randint(60, 144)

        # 5) battery
        battery = random.randint(3000, 6000)

        # 6) price
        price = random.randint(3000, 40000)

        # 7) ram
        ram = random.randint(3, 16)

        # 8) size, float with 2 decimal values
        size = round(random.uniform(4.5, 7.2), 2)

        # 9) camera_res - float with 1 decimal value
        camera_res = round(random.uniform(8.0, 108.0), 1)

        phone = {
            "model": model_name,
            "display_freq": display_freq,
            "battery": battery,
            "price": price,
            "ram": ram,
            "size": size,
            "camera_res": camera_res
        }
        phones.append(phone)

    return phones


def write_csv(phones, output_file="phones.csv"):
    """insert data into CSV"""
    fieldnames = [
        "model", "display_freq", "battery",
        "price", "ram", "size", "camera_res"
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in phones:
            writer.writerow(row)


def main():
    """Parse the arguments and start generating the dataset."""
    parser = argparse.ArgumentParser(description="Generuje dataset mobilních telefonů do CSV.")
    parser.add_argument("--count", type=int, default=30,
                        help="Počet řádků vygenerovaných do datasetu.")
    parser.add_argument("--output", type=str, default="phones_generated.csv",
                        help="Výstupní CSV soubor.")
    args = parser.parse_args()

    # 1) generate list of phones
    phones = generate_phones(args.count)
    # 2) write into CSV
    write_csv(phones, args.output)
    print(f"Vygenerováno {args.count} záznamů do souboru {args.output}")


if __name__ == "__main__":
    main()
