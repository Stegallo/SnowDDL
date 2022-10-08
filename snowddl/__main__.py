from .app.base import BaseApp, Configuration


def main():
    ...
    config = {
        "path": "CONFIG",
        "account": "account",
        "user": "str",
        "password": "str",
        "role": "role",
        "warehouse": "warehouse",
        "action": "plan",
    }
    c = Configuration(**config)
    BaseApp(c)
    # app.execute()


if __name__ == "__main__":
    main()
