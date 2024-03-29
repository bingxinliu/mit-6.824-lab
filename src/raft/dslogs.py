#!/usr/bin/env python
import sys
import shutil
from typing import Optional, List, Tuple, Dict

import typer
from rich import color, print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "APDE": "#9a9a99",
    "RQVT": "#67a0b2",
    "ISNP": "#67a0b2",
    "HTBT": "#d0b343",
    "TERM": "#70c43f",
    "VOTE": "#4878bc",
    "ROLE": "#398280",
    "CMIT": "#98719f",
    "APLY": "#d08341",
    "PERS": "#FD971F",
    "SNAP": "#ff615c",
    "TIME": "#00813c",
    "TRCE": "#fe2c79",
    "CNST": "#ffffff",
    "STAT": "#d08341",
    "INFO": "#fe2626",
    "WARN": "#fe2626",
}
# fmt: on

def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics

def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize: bool = typer.Option(True, "--no-color"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
    ignore: Optional[str] = typer.Option(None, "-ignore", "-i", callback=list_topics),
    just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
    ):
    topics = list(TOPICS)

    input_ = file if file else sys.stdin

    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        try:
            time, topic, term, role, *msg = line.strip().split(" ")
            if topic not in topics:
                continue

            msg = " ".join(msg)

            i = 0
            if topic != "TEST":
                i = int(msg[0])


            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"{time} TERM{term} [{color}]{msg}[/{color}]"

            if n_columns is None or topic == "TEST":
                print(time, msg)
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                print(cols)

        except:
            if line.startswith("panic"):
                panic = True

            if not panic:
                print("#" * console.width)

            print(line, end="")

if __name__ == "__main__":
    typer.run(main)














