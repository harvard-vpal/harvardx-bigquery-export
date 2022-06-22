import datetime
import json
import os
import sys
from hashlib import sha1

import base_repr
import pandas as pd
import pandas_gbq
import tomli
from dotenv import load_dotenv


# set some display options
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 50)
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.width", 150)
pd.set_option("display.max_rows", 50)
pd.set_option("mode.chained_assignment", "raise")


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

            try:
                df = pandas_gbq.read_gbq(q)

                print("Exploding JSON columns...")
                for c in t["json_cols"]:
                    df = df.join(df[c].apply(json.loads).apply(pd.Series))
                    df = df.drop(columns=[c])

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

            except Exception as e:
                print(e)

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
