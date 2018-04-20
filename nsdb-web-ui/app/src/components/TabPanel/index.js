import React from 'react';
import get from 'lodash/get';
import Tabs from 'antd/lib/tabs';
import Button from 'antd/lib/button';

import './index.less';

class TabPanel extends React.Component {
  static defaultProps = {
    onAdd: () => undefined,
    onRemove: () => undefined,
    minPanes: 0,
    maxPanes: Number.MAX_SAFE_INTEGER,
    text: 'Add',
    panes: [],
  };

  constructor(props) {
    super(props);
    this.state = {
      activeKey: get(props.panes[0], 'props.id', null),
    };
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.panes !== nextProps.panes) {
      if (this.props.panes.length < nextProps.panes.length) {
        const newActiveKey = nextProps.panes[nextProps.panes.length - 1].props.id;
        this.setState({ activeKey: newActiveKey });
      }
    }
  }

  onChange = activeKey => this.setState({ activeKey });

  onEdit = (targetKey, action) => {
    this[action](targetKey);
  };

  add = e => {
    e.preventDefault();
    const { maxPanes, panes, onAdd } = this.props;
    if (panes.length < maxPanes) {
      onAdd();
    }
  };

  remove = targetKey => {
    const { minPanes, panes, onRemove } = this.props;
    const { activeKey } = this.state;
    if (panes.length > minPanes) {
      const newActiveKey = activeKey === targetKey ? this.getNewKey(panes, targetKey) : activeKey;
      this.setState({ activeKey: newActiveKey }, () => onRemove(targetKey));
    }
  };

  getNewKey = (panes, key) => {
    const index = panes.findIndex(pane => pane.props.id === key);
    if (index !== -1 && panes.length > 1) {
      if (index === 0) {
        return panes[1].props.id;
      } else {
        return panes[index - 1].props.id;
      }
    }
    return null;
  };

  render() {
    const { text, panes } = this.props;
    const { activeKey } = this.state;

    return (
      <div className="TabPanel">
        <div className="TabPanel-button">
          <Button onClick={this.add}>{text}</Button>
        </div>
        <Tabs
          className="TabPanel-tabs"
          type="editable-card"
          hideAdd
          activeKey={activeKey}
          onChange={this.onChange}
          onEdit={this.onEdit}
        >
          {panes}
        </Tabs>
      </div>
    );
  }
}

export default TabPanel;
