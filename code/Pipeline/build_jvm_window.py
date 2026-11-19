"""Compile the JVM window operator against the installed PyFlink distribution."""

import argparse
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import pyflink


def build_jar(output: Path):
    flink_libraries = Path(pyflink.__file__).parent / "lib" / "*"
    source = Path(__file__).with_name("jvm") / "PriceWindows.java"
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="ssdm-jvm-") as classes:
        subprocess.run(["javac", "-cp", str(flink_libraries), "-d", classes, str(source)], check=True)
        subprocess.run(["jar", "--create", "--file", str(output.resolve()), "-C", classes, "."], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build_jar(parser.parse_args().output)


if __name__ == "__main__":
    main()
