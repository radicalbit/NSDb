import React from 'react';
import { hot } from 'react-hot-loader';
import { connect } from 'react-redux';
import { push } from 'react-router-redux';
import { Form } from 'react-form';
import AntdLayout from 'antd/lib/layout';
import { Row, Col } from 'antd/lib/grid';
import Card from 'antd/lib/card';
import Button from 'antd/lib/button';
import { notEmpty } from '../../utils/validations';

import Logo from '../../components/Logo';
import FormInputText from '../../components/FormInputText';

import nsdbLogo from '../../assets/images/logo.svg';
import './index.less';

const AntdContent = AntdLayout.Content;

class Login extends React.Component {
  handleSubmitSuccess = () => {
    const { location, pushHistory } = this.props;
    if (location.state && location.state.from) {
      pushHistory(this.props.location.state.from);
    } else {
      pushHistory('/');
    }
  };

  handleSubmitFailure = () => console.log('submit failure');

  render() {
    return (
      <div className="Login">
        <AntdLayout>
          <AntdContent>
            <Row type="flex" align="middle" justify="center">
              <Col span={8}>
                <Card className="Login-card" title={<Logo image={nsdbLogo} alt="NSDB" />}>
                  <Form
                    onSubmit={this.handleSubmitSuccess}
                    onSubmitFailure={this.handleSubmitFailure}
                  >
                    {formApi => (
                      <form onSubmit={formApi.submitForm}>
                        <Row>
                          <Col span={11}>
                            <FormInputText label="Host" field="host" validate={notEmpty} />
                          </Col>
                          <Col span={11} offset={2}>
                            <FormInputText label="Port" field="port" validate={notEmpty} />
                          </Col>
                        </Row>
                        <FormInputText label="Database" field="database" validate={notEmpty} />
                        <Button htmlType="submit">Login</Button>
                      </form>
                    )}
                  </Form>
                </Card>
              </Col>
            </Row>
          </AntdContent>
        </AntdLayout>
      </div>
    );
  }
}

const mapDispatchToProps = {
  pushHistory: push,
};

export default hot(module)(connect(null, mapDispatchToProps)(Login));
