import React from 'react';
import { connect } from 'react-redux';
import { createStructuredSelector } from 'reselect';
import ReactTable from 'react-table';
import { Row, Col } from 'antd/lib/grid';

import { actions as nsdbActions, selectors as nsdbSelectors } from '../../store/nsdb';

import './index.less';

class QueryResult extends React.Component {
  constructor(props) {
    super(props);
    this.columns = [
      {
        Header: 'Series',
        columns: [
          {
            Header: 'Timestamp',
            accessor: 'timestamp',
          },
          {
            Header: 'Value',
            accessor: 'value',
          },
        ],
      },
    ];
  }

  componentWillReceiveProps(nextProps) {
    if (
      nextProps.metricDescription.length > 0 &&
      this.props.metricDescription !== nextProps.metricDescription
    ) {
      const dimensionColumns = nextProps.metricDescription.map(description => ({
        Header: `Dimension ${description.name}`,
        id: `Dimension ${description.name}`,
        accessor: record => record.dimensions[description.name],
      }));
      this.columns[1] = {
        Header: 'Dimensions',
        columns: dimensionColumns,
      };
    }
  }

  componentWillUnmount() {
    const { id, removeResult } = this.props;
    removeResult(id);
  }

  render() {
    const { records } = this.props;

    return (
      <div className="QueryResult">
        <Row>
          <Col>
            <ReactTable
              data={records}
              columns={this.columns}
              showPaginationTop={true}
              showPaginationBottom={false}
              defaultPageSize={5}
            />
          </Col>
        </Row>
      </div>
    );
  }
}

const mapStateToProps = createStructuredSelector({
  database: nsdbSelectors.getSelectedDatabase,
  metricDescription: nsdbSelectors.getMetricDescription,
  records: nsdbSelectors.getRecords,
});

const mapDispatchToProps = {
  removeResult: nsdbActions.removeResult,
};

export default connect(mapStateToProps, mapDispatchToProps)(QueryResult);
