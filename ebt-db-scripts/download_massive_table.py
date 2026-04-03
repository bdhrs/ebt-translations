from ebt_translations.downloads import download_massive_table


def main() -> None:
    path = download_massive_table()
    print(f"Downloaded spreadsheet to {path}")


if __name__ == "__main__":
    main()
