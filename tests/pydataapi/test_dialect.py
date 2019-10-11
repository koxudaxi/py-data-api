import pytest
from sqlalchemy.engine import ResultProxy


@pytest.fixture
def mocked_client(mocker):
    return mocker.patch('boto3.client')


def test_mysql(mocked_client) -> None:
    from sqlalchemy.engine import create_engine

    mocked_client.execute_statement.side_effect = [
        {'records': [[{'stringValue': 'test plain returns'}]]},
        {'records': [[{'stringValue': 'test unicode returns'}]]},
        {
            'numberOfRecordsUpdated': 0,
            'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
            "columnMetadata": [
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "id",
                    "name": "id",
                    "nullable": 1,
                    "precision": 11,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 4,
                    "typeName": "INT",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": False,
                    "label": "name",
                    "name": "name",
                    "nullable": 1,
                    "precision": 255,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 12,
                    "typeName": "VARCHAR",
                },
            ],
        },
    ]
    engine = create_engine(
        'mysql+pydataapi://',
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test',
            'client': mocked_client,
        },
    )

    result: ResultProxy = engine.execute("select * from pets")
    assert result.fetchall() == [(1, 'cat')]


def test_postgresql(mocked_client) -> None:
    from sqlalchemy.engine import create_engine

    mocked_client.execute_statement.side_effect = [
        {'records': [[{'stringValue': 'test plain returns'}]]},
        {'records': [[{'stringValue': 'test unicode returns'}]]},
        {
            'numberOfRecordsUpdated': 0,
            'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
            "columnMetadata": [
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "id",
                    "name": "id",
                    "nullable": 1,
                    "precision": 11,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 4,
                    "typeName": "INT",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": False,
                    "label": "name",
                    "name": "name",
                    "nullable": 1,
                    "precision": 255,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 12,
                    "typeName": "VARCHAR",
                },
            ],
        },
    ]
    engine = create_engine(
        'postgresql+pydataapi://',
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test',
            'client': mocked_client,
        },
    )

    result: ResultProxy = engine.execute("select * from pets")
    assert result.fetchall() == [(1, 'cat')]
