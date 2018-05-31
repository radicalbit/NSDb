import React from 'react';
import { hot } from 'react-hot-loader';
import { connect } from 'react-redux';
import { createStructuredSelector } from 'reselect';
import Tabs from 'antd/lib/tabs';

import { actions as nsdbActions, selectors as nsdbSelectors } from '../../store/nsdb';

import QueryBuilder from '../../containers/QueryBuilder';
import QueryResult from '../../containers/QueryResult';
import TabPanel from '../../components/TabPanel';

import './index.less';

const TabPane = Tabs.TabPane;

class Nsdb extends React.Component {
  componentWillMount() {
    const { fetchDatabasesRequest } = this.props;
    fetchDatabasesRequest();
  }

  render() {
    const { tabs, addTab, removeTab } = this.props;

    return (
      <div className="Nsdb">
        <TabPanel
          onAdd={addTab}
          onRemove={removeTab}
          text="Add Query"
          minPanes={1}
          panes={tabs.map((tab, index) => (
            <TabPane key={tab.id} id={tab.id} tab={tab.title}>
              <QueryBuilder key={`query-builder-${tab.id}`} id={tab.id} />
              <br />
              <QueryResult key={`query-result-${tab.id}`} id={tab.id} />
            </TabPane>
          ))}
        />
      </div>
    );
  }
}

const mapStateToProps = createStructuredSelector({
  tabs: nsdbSelectors.getTabs,
});

const mapDispatchToProps = {
  addTab: nsdbActions.addTab,
  removeTab: nsdbActions.removeTab,
  fetchDatabasesRequest: nsdbActions.fetchDatabasesRequest,
};

export default hot(module)(connect(mapStateToProps, mapDispatchToProps)(Nsdb));
