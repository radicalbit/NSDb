import React from 'react';
import { connect } from 'react-redux';
import { createStructuredSelector } from 'reselect';
import Table from 'antd/lib/table';

import { selectors as authSelectors } from '../../store/auth';
import { actions as nsdbActions, selectors as nsdbSelectors } from '../../store/nsdb';

import './index.less';

class MetricTable extends React.Component {
  componentWillReceiveProps(nextProps) {
    const { database, namespace, metric, fetchMetricDescriptionRequest } = nextProps;
    if (database && namespace && metric && this.props.metric !== metric) {
      fetchMetricDescriptionRequest(database, namespace, metric);
    }
  }

  columns = [
    { title: 'Dimension Name', dataIndex: 'name', width: '50%' },
    { title: 'Type', dataIndex: 'type', width: '50%' },
  ];

  render() {
    const { metricDescription } = this.props;

    return (
      <div className="MetricTable">
        <Table
          bordered
          size="small"
          dataSource={metricDescription}
          columns={this.columns}
          rowKey={record => record.name}
          rowClassName={(record, index) => (index % 2 === 0 ? 'row-even' : 'row-odd')}
          pagination={false}
          scroll={{ x: true, y: 343 }}
        />
      </div>
    );
  }
}

const mapStateToProps = createStructuredSelector({
  metricDescription: nsdbSelectors.getMetricDescription,
});

const mapDispatchToProps = {
  fetchMetricDescriptionRequest: nsdbActions.fetchMetricDescriptionRequest,
};

export default connect(mapStateToProps, mapDispatchToProps)(MetricTable);
