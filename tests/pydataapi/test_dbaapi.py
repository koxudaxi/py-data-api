import pytest
from pydataapi import connect


@pytest.fixture
def mocked_client(mocker):
    return mocker.patch('boto3.client')


def test_resource_arn(mocker, mocked_client) -> None:
    mock_client = mocker.Mock()
    mock_client.describe_db_clusters.return_value = {
        'DBClusters': [{'DBClusterArn': 'arn:aws:rds:dummy'}]
    }
    data_api = connect(
        resource_name='dummy',
        secret_arn='dummy',
        client=mock_client,
        rds_client=mock_client,
    )
    assert data_api._data_api.resource_arn == 'arn:aws:rds:dummy'

    mocked_client.return_value = mock_client

    data_api = connect(
        resource_arn='arn:aws:rds:dummy', secret_arn='dummy', client=mock_client
    )
    assert data_api._data_api.resource_arn == 'arn:aws:rds:dummy'


def test_commit(mocked_client, mocker) -> None:
    mocked_client.commit_transaction.return_value = {'transactionStatus': 'abc'}

    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
        transaction_id='abc',
    )
    data_api.commit()
    assert mocked_client.commit_transaction.call_args == mocker.call(
        resourceArn='arn:aws:rds:dummy', transactionId='abc', secretArn='dummy'
    )


def test_commit_not_called(mocked_client, mocker) -> None:

    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    data_api.commit()
    mocked_client.commit_transaction.assert_not_called()


def test_rollback(mocked_client, mocker) -> None:
    mocked_client.rollback_transaction.return_value = {'transactionStatus': 'abc'}
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
        transaction_id='abc',
    )
    data_api.rollback()
    assert mocked_client.rollback_transaction.call_args == mocker.call(
        resourceArn='arn:aws:rds:dummy', transactionId='abc', secretArn='dummy'
    )


def test_rollback_not_called(mocked_client) -> None:
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    data_api.rollback()
    mocked_client.rollback_transaction.assert_not_called()


def test_execute_insert(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.return_value = {
        'generatedFields': [{'longValue': 3}],
        'numberOfRecordsUpdated': 1,
    }
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    results = data_api.execute("insert into pets values(1, 'cat')")
    assert list(results.fetchall()) == []
    assert results.lastrowid == 3
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql="insert into pets values(1, 'cat')",
        database='test',
        transactionId='abc',
    )


def test_execute_insert_parameters(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.return_value = {
        'generatedFields': [],
        'numberOfRecordsUpdated': 1,
    }
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    results = data_api.execute(
        "insert into pets values(:id, :name)", {'id': 1, 'name': 'cat'}
    )
    assert list(results.fetchall()) == []
    # assert results.number_of_records_updated == 1
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        includeResultMetadata=True,
        parameters=[
            {'name': 'id', 'value': {'longValue': 1}},
            {'name': 'name', 'value': {'stringValue': 'cat'}},
        ],
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql="insert into pets values(:id, :name)",
        database='test',
        transactionId='abc',
    )


def test_execute_select(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.return_value = {
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
    }
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    result = data_api.cursor().execute("select * from pets")
    assert result.rowcount == 1
    assert result.fetchone() == [1, 'cat']
    assert result.fetchone() is None
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql='select * from pets',
        transactionId='abc',
    )

    data_api.close()
    assert data_api.closed is True


def test_execute_select_fetch_many(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.return_value = {
        'numberOfRecordsUpdated': 0,
        'records': [
            [{'longValue': 1}, {'stringValue': 'cat'}],
            [{'longValue': 2}, {'stringValue': 'dog'}],
            [{'longValue': 3}, {'stringValue': 'snake'}],
        ],
    }
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    result = data_api.cursor().execute("select * from pets")
    assert result.rowcount == 3
    assert result.fetchmany(2) == [[1, 'cat'], [2, 'dog']]
    assert result.fetchmany() == [[3, 'snake']]
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql='select * from pets',
        transactionId='abc',
    )

    data_api.close()
    assert data_api.closed is True


def test_execute_select_iter(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.return_value = {
        'numberOfRecordsUpdated': 0,
        'records': [
            [{'longValue': 1}, {'stringValue': 'cat'}],
            [{'longValue': 2}, {'stringValue': 'dog'}],
            [{'longValue': 3}, {'stringValue': 'snake'}],
        ],
    }
    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    result = data_api.cursor().execute("select * from pets")
    result_iter = iter(result)
    assert next(result_iter) == [1, 'cat']
    assert next(result_iter) == [2, 'dog']
    assert next(result_iter) == [3, 'snake']
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql='select * from pets',
        transactionId='abc',
    )

    data_api.close()
    assert data_api.closed is True


def test_execute_insert_parameter_set(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.batch_execute_statement.return_value = {
        'updateResults': [
            {'generatedFields': [{'longValue': 3}]},
            {'generatedFields': [{'longValue': 4}]},
        ]
    }

    data_api = connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    results = data_api.cursor().executemany(
        "insert into test.pets  values (:id , :name)",
        [{'id': 3, 'name': 'bird'}, {'id': 4, 'name': 'lion'}],
    )
    rows = results.fetchall()
    assert len(rows) == 2
    assert rows == [[3], [4]]
    assert results.lastrowid == 4

    assert mocked_client.batch_execute_statement.call_args == mocker.call(
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql="insert into test.pets  values (:id , :name)",
        parameterSets=[
            [
                {'name': 'id', 'value': {'longValue': 3}},
                {'name': 'name', 'value': {'stringValue': 'bird'}},
            ],
            [
                {'name': 'id', 'value': {'longValue': 4}},
                {'name': 'name', 'value': {'stringValue': 'lion'}},
            ],
        ],
        database='test',
        transactionId='abc',
    )


def test_with_statement(mocked_client) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with connect(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    ):
        mocked_client.begin_transaction.assert_called_once_with(
            database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
        )
    mocked_client.commit_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )


def test_with_statement_exception(mocked_client) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(Exception):
        with connect(
            resource_arn='arn:aws:rds:dummy',
            secret_arn='dummy',
            database='test',
            client=mocked_client,
        ):
            mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
            )
            raise Exception('error')
    mocked_client.rollback_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )


def test_with_statement_custom_exception(mocked_client, mocker) -> None:
    class CustomError(Exception):
        pass

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(CustomError):
        with connect(
            resource_arn='arn:aws:rds:dummy',
            secret_arn='dummy',
            database='test',
            client=mocked_client,
            rollback_exception=CustomError,
        ):
            mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
            )
            raise CustomError('error')
    mocked_client.rollback_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )

    second_mocked_client = mocker.patch('boto3.client')
    second_mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(Exception):
        with connect(
            resource_arn='arn:aws:rds:dummy',
            secret_arn='dummy',
            database='test',
            client=second_mocked_client,
            rollback_exception=CustomError,
        ):
            second_mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
            )
            raise Exception('error')
    second_mocked_client.rollback_transaction.assert_not_called()
