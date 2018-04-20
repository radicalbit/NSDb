import React from 'react';
import { connect } from 'react-redux';
import { push, goBack } from 'react-router-redux';
import { Row, Col } from 'antd/lib/grid';
import Button from 'antd/lib/button';

import './index.less';

class NotFound extends React.Component {
  handleClickBack = () => this.props.goBackHistory();

  handleClickHome = () => this.props.pushHistory('/');

  render() {
    return (
      <div className="NotFound">
        <Row type="flex" align="middle" justify="center">
          <Col span={8}>
            <h1 className="NotFound-message">404 Not Found</h1>
            <Row>
              <Col span={6} offset={6}>
                <Button onClick={this.handleClickBack}>Go back</Button>
              </Col>
              <Col span={12}>
                <Button onClick={this.handleClickHome}>Go home</Button>
              </Col>
            </Row>
          </Col>
        </Row>
      </div>
    );
  }
}

const mapDispatchToProps = {
  pushHistory: push,
  goBackHistory: goBack,
};

export default connect(null, mapDispatchToProps)(NotFound);
