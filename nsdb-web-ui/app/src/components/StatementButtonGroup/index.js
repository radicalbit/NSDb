import React from 'react';
import Button from 'antd/lib/button';
import Icon from 'antd/lib/icon';

import './index.less';

const ButtonGroup = Button.Group;

const StatementButtonGroup = ({ onHistorical, onRealtime, onInsert, onDelete }) => (
  <div className="StatementButtonGroup">
    <ButtonGroup>
      <Button onClick={onHistorical}>
        <Icon type="calendar" />Historical
      </Button>
      <Button onClick={onRealtime}>
        <Icon type="clock-circle-o" />Realtime
      </Button>
      <Button onClick={onInsert}>
        <Icon type="plus-circle-o" />Insert
      </Button>
      <Button onClick={onDelete}>
        <Icon type="close-circle-o" />Delete
      </Button>
    </ButtonGroup>
  </div>
);

export default StatementButtonGroup;
