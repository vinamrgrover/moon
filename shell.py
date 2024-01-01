from flask import Flask, request
from moon import MoonBase, MoonQuery
from argparse import ArgumentParser
from rich.table import Table
import pandas as pd
import rich
import os

app = Flask("moonQL API")
console = rich.get_console()


if __name__ == "__main__":
    if "saved" not in os.listdir():
        os.mkdir("saved")
        saved_abs_path = os.path.abspath("saved")
        console.print(f"Created an empty directory - {saved_abs_path}")

    saved_abs_path = os.path.abspath("saved")

    parser = ArgumentParser()
    parser.add_argument("-H", "--host")
    parser.add_argument("-d", "--database")
    parser.add_argument("-u", "--user")
    parser.add_argument("-p", "--password")
    parser.add_argument('-n', '--port')
    args = parser.parse_args()

    missing_args = [key for key, val in vars(args).items() if val is None]

    # Exiting if arguments are missing
    if not all(vars(args).values()):
        console.print(
            f"""
            Please re-enter all the required arguments!
            Missing Arguments: {','.join(missing_args)}
            
            """, style="red"
        )
        exit(1)

    try:
        m = MoonQuery(
            host=args.host,
            db=args.database,
            user=args.user,
            password=args.password,
            port=args.port
        )

        console.print(
            f"Successfully attached DB - {args.database} to DuckDB!",
            style="green",
        )

        console.print('Welcome to Moon 🌙\n', style = 'cyan')
        console.print('Inspired and built on top of DuckDB 🐤', style = 'cyan')
        console.print('Start executing queries against your DB 👇', style = 'cyan')


        while True:

            query = input('>>> ')
            if query:
                if query == 'exit':
                    exit(0)

                df = m.view_endpoint(query)
                if not type(df) == pd.DataFrame:
                    console.print(f'Invalid Query Syntax!\n{df}', style = 'magenta')
                    continue

                table = Table()

                # Adding cols to table
                for col in df.columns:
                    table.add_column(col, style = 'yellow')
                # Adding rows to table
                for row in df.values.tolist():
                    table.add_row(*[str(x) for x in row], style='cyan')

                console.print(table)

    except Exception as e:
        raise e
