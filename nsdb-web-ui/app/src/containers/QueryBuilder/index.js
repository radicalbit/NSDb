import React from 'react';
import { connect } from 'react-redux';
import { createStructuredSelector } from 'reselect';
import { Form } from 'react-form';
import get from 'lodash/get';
import { Row, Col } from 'antd/lib/grid';
import Button from 'antd/lib/button';
import Icon from 'antd/lib/icon';
import { notEmpty } from '../../utils/validations';

import { actions as nsdbActions, selectors as nsdbSelectors } from '../../store/nsdb';

import MetricTable from '../../containers/MetricTable';
import StatementButtonGroup from '../../components/StatementButtonGroup';
import FormSelect from '../../components/FormSelect';
import FormSqlCodeMirror from '../../components/FormSqlCodeMirror';

import './index.less';

const queryTemplates = {
  historical: 'SELECT * FROM ... LIMIT 100',
  realtime: 'SELECT * FROM ... ORDER BY timestamp LIMIT 1',
  insert: 'INSERT ...',
  delete: 'DELETE ...',
};

class QueryBuilder extends React.Component {
  constructor(props) {
    super(props);
    this.queryType = 'historical';
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.selectedNamespace !== nextProps.selectedNamespace) {
      const { selectedDatabase, selectedNamespace, fetchMetricsRequest } = nextProps;
      this.formApi.setValue('metric', null);
      this.formApi.setTouched('metric', false);
      fetchMetricsRequest(selectedDatabase, selectedNamespace);
    }

    if (this.props.selectedDatabase !== nextProps.selectedDatabase) {
      const { selectedDatabase, fetchNamespaceRequest } = nextProps;
      this.formApi.setValue('namespace', null);
      this.formApi.setTouched('namespace', false);
      fetchNamespaceRequest(selectedDatabase);
    }
  }

  getFormApi = formApi => (this.formApi = formApi);

  onDatabaseChange = id => value => this.props.selectDatabase(id, value);

  onNamespaceChange = id => value => this.props.selectNamespace(id, value);

  onMetricChange = id => value => this.props.selectMetric(id, value);

  handleSetQuery = (formApi, queryType) => {
    this.queryType = queryType;
    formApi.setValue('query', queryTemplates[queryType]);
  };

  handleSubmitSuccess = (values, e, formApi) => {
    const { id, fetchHistoricalQueryRequest, startRealtimeQuerySocket } = this.props;
    switch (this.queryType) {
      case 'historical': {
        console.log('Running historical query');
        const query = {
          db: values.database,
          namespace: values.namespace,
          metric: values.metric,
          queryString: values.query,
        };
        fetchHistoricalQueryRequest(id, query);
        break;
      }
      case 'realtime': {
        console.log('Running realtime query');
        const query = {
          db: values.database,
          namespace: values.namespace,
          metric: values.metric,
          queryString: values.query,
        };
        startRealtimeQuerySocket(id, query);
        break;
      }
      case 'insert': {
        console.log('Running insert query');
        break;
      }
      case 'delete': {
        console.log('Running delete query');
        break;
      }
      default:
    }
  };

  handleSubmitFailure = () => console.log('submit failure');

  render() {
    const {
      id,
      databasesAllNames,
      namespacesByDatabase,
      metricsByNamespace,
      stopRealtimeQuerySocket,
    } = this.props;

    return (
      <div className="QueryBuilder">
        <Form
          pure={false}
          onSubmit={this.handleSubmitSuccess}
          onSubmitFailure={this.handleSubmitFailure}
          getApi={this.getFormApi}
        >
          {formApi => (
            <form onSubmit={formApi.submitForm}>
              <Row>
                <Col span={7}>
                  <FormSelect
                    options={databasesAllNames}
                    label="Database"
                    field="database"
                    validate={notEmpty}
                    onChange={this.onDatabaseChange(id)}
                  />
                </Col>
                <Col span={7} offset={1}>
                  <FormSelect
                    options={namespacesByDatabase}
                    label="Namespace"
                    field="namespace"
                    validate={notEmpty}
                    onChange={this.onNamespaceChange(id)}
                  />
                </Col>
                <Col span={7} offset={2}>
                  <FormSelect
                    options={metricsByNamespace}
                    label="Metric"
                    field="metric"
                    validate={notEmpty}
                    onChange={this.onMetricChange(id)}
                  />
                </Col>
              </Row>
              <Row>
                <Col span={11}>
                  <StatementButtonGroup
                    onHistorical={() => this.handleSetQuery(formApi, 'historical')}
                    onRealtime={() => this.handleSetQuery(formApi, 'realtime')}
                    onInsert={() => this.handleSetQuery(formApi, 'insert')}
                    onDelete={() => this.handleSetQuery(formApi, 'delete')}
                  />
                  <FormSqlCodeMirror field="query" validate={notEmpty} />
                  <Button htmlType="submit">
                    Run<Icon type="play-circle" />
                  </Button>
                  <Button onClick={() => stopRealtimeQuerySocket(id)}>
                    Stop<Icon type="close-circle" />
                  </Button>
                </Col>
                <Col span={11} offset={1}>
                  <MetricTable
                    id={id}
                    database={get(formApi.values, 'database', null)}
                    namespace={get(formApi.values, 'namespace', null)}
                    metric={get(formApi.values, 'metric', null)}
                  />
                </Col>
              </Row>
            </form>
          )}
        </Form>
      </div>
    );
  }
}

const mapStateToProps = createStructuredSelector({
  database: nsdbSelectors.getIsFetchingDatabases,
  isFetchingNamespaces: nsdbSelectors.getIsFetchingNamespaces,
  isFetchingMetrics: nsdbSelectors.getIsFetchingMetrics,
  selectedDatabase: nsdbSelectors.getSelectedDatabase,
  selectedNamespace: nsdbSelectors.getSelectedNamespace,
  selectedMetric: nsdbSelectors.getSelectedMetric,
  databasesAllNames: nsdbSelectors.getDatabasesAllNames,
  namespacesByDatabase: nsdbSelectors.getNamespacesByDatabase,
  metricsByNamespace: nsdbSelectors.getMetricsByNamespace,
});

const mapDispatchToProps = {
  selectDatabase: nsdbActions.selectDatabase,
  selectNamespace: nsdbActions.selectNamespace,
  selectMetric: nsdbActions.selectMetric,
  fetchMetricsRequest: nsdbActions.fetchMetricsRequest,
  fetchNamespaceRequest: nsdbActions.fetchNamespacesRequest,
  fetchHistoricalQueryRequest: nsdbActions.fetchHistoricalQueryRequest,
  startRealtimeQuerySocket: nsdbActions.startRealtimeQuerySocket,
  stopRealtimeQuerySocket: nsdbActions.stopRealtimeQuerySocket,
};

export default connect(mapStateToProps, mapDispatchToProps)(QueryBuilder);
