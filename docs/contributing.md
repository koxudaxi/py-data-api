# Contributing
We are waiting for your contributions to `pydataapi`.

## How to contribute
```shell
## 1. Clone your fork repository
$ git clone git@github.com:<your username>/py-data-api.git
$ cd py-data-api

## 2. Create `venv` with python3.7 (also you should do with python3.6)
$ python3.7 -m venv venv37
$ source venv37/bin/activate  

## 3. Install dependencies
$ python3 -m pip install ".[all]" 

## 4. Create new branch and rewrite code.
$ git checkout -b new-branch

## 5. Run unittest (you should pass all test and coverage should be 100%)
$ ./scripts/test.sh

## 6. Format code
$ ./scripts/format.sh

## 7. Check lint (mypy)
$ ./scripts/lint.sh

## 8. Commit and Push...
```
