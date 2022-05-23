import datetime
import os
import sys
from hashlib import sha1

import base_repr
import pandas_gbq
import tomli
from dotenv import load_dotenv


def main() -> int:
    # configure
    load_dotenv()
    with open("config.toml", "rb") as f:
        config = tomli.load(f)

    print(f"Found {len(config['course_ids'])} course IDs.")

    for course_id in config["course_ids"]:
        for t in config["table"]:
            # construct BigQuery table name
            full_table_name = ".".join(
                [
                    os.environ["GOOGLE_PROJECT_NAME"],
                    "_".join([course_id, "latest"]),
                    t["table_name"],
                ]
            )

            # get results as DataFrame
            print(f"Querying {full_table_name}...")
            q = t["q"].replace("full_table_name", full_table_name)
            df = pandas_gbq.read_gbq(q)

            # transform PII columns
            print("Anonymizing...")
            for c in t["pii_cols"]:
                # base62-encode the sha1 hash of the column
                df[c] = (
                    df[c]
                    .astype("string")
                    .apply(
                        lambda x: base_repr.bytes_to_repr(
                            sha1(x.encode()).digest(), base=62
                        )
                    )
                )

            # make output directory
            outdir = os.path.join(
                "exported", datetime.date.today().isoformat(), course_id
            )
            os.makedirs(outdir, exist_ok=True)

            # write CSV
            f = os.path.join(outdir, ".".join([t["table_name"], "csv"]))
            print(f"Writing {f}...")
            df.to_csv(f, index=False)

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
