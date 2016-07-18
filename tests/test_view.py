import pytest

from PyQt4 import QtCore

from query_manager import Field, QueryManager
from view import Criteria, DatasheetView, Filter, MainView, QueryDesigner

# QUERY DESIGNER TESTS
@pytest.fixture(scope='module')
def fields():
    return [
         Field('CustomerID', 'int')
         , Field('ProductID', 'int')
         , Field('CustomerName', 'str')
         , Field('OrderDate', 'date')
         , Field('ShippingDate', 'date')
         , Field('ShippingAddress', 'str')
         , Field('SalesAmount', 'float')
     ]

@pytest.fixture(scope='module')
def filters():
    return [
        Filter(field='CustomerName', operator='Starts with', type='str')
        , Filter(field='OrderDate', operator='<=', type='date')
        , Filter(field='OrderDate', operator='>=', type='date')
        , Filter(field='SalesAmount', operator='+/- 0.01', type='float')
    ]


@pytest.fixture(scope='module')
def query_designer(filters):
    qd = QueryDesigner(filters=filters, max_display_rows=1000, max_export_rows=5000)
    qd.show()
    return qd


@pytest.fixture(scope='module')
def datasheet_view(fields, filters):
    config = {
        'db': 'test.db'
        , 'fields': fields
        , 'filters': filters
        , 'table': 'Customers'
        , 'max_export_rows': 500000
        , 'max_display_rows': 1000
        , 'order_by': ''
    }
    return QueryManager(config)


@pytest.fixture(scope='module')
def text_box(query_designer, filters):
    return query_designer.query_controls[
        filters[0].field + '_' + filters[0].type + '_' + filters[0].operator
    ].handle


@pytest.fixture(scope='module')
def query_manager():
    fields = [
        Field('CustomerID', 'int')
        , Field('ProductID', 'int')
        , Field('CustomerName', 'str')
        , Field('OrderDate', 'date')
        , Field('ShippingDate', 'date')
        , Field('ShippingAddress', 'str')
        , Field('SalesAmount', 'float')
    ]
    config = {
        'db': 'test.db'
        , 'fields': fields
        , 'filters': []
        , 'table': 'Customers'
        , 'max_export_rows': 500000
        , 'max_display_rows': 1000
        , 'order_by': ''
    }
    return QueryManager(config)


def test_reset_button(qtbot, query_designer):
    qtbot.addWidget(query_designer)
    with qtbot.waitSignal(query_designer.reset_signal, raising=True):
        qtbot.mouseClick(query_designer.btn_reset_query, QtCore.Qt.LeftButton)


def test_add_criteria(qtbot, query_designer, filters, text_box):
    qtbot.addWidget(query_designer)
    with qtbot.waitSignal(query_designer.add_criteria_signal) as blocker:
        qtbot.keyClicks(text_box, 'a')
    assert blocker.args[0] == Criteria(
        field_name='CustomerName'
        , value='a'
        , field_type='str'
        , operator='Starts with'
    )

if __name__ == '__main__':
    pytest.main(__file__)


