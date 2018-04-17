import React from 'react';
import { hot } from 'react-hot-loader';
import { Switch, Route } from 'react-router';
import { ConnectedRouter } from 'react-router-redux';
import { connect } from 'react-redux';

import Layout from '../../components/Layout';
import MultiRoute from '../../components/MultiRoute';
import RestrictedRoute from '../../components/RestrictedRoute';

import './index.less';

class App extends React.Component {
  render() {
    const { history, loginRoute, restrictedRoutes } = this.props;

    return (
      <ConnectedRouter history={history}>
        <Switch>
          <Route exact path={loginRoute.path} component={loginRoute.component} />
          <RestrictedRoute
            isLoggedIn
            fallback="/login"
            render={props => (
              <Layout>
                <MultiRoute path="/" routes={restrictedRoutes} fallback="/404" />
              </Layout>
            )}
          />
        </Switch>
      </ConnectedRouter>
    );
  }
}

export default hot(module)(connect()(App));
