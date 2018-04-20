import React from 'react';
import { Switch, Route, Redirect } from 'react-router';

const MultiRoute = ({ routes, fallback, ...rest }) => (
  <Route
    {...rest}
    render={props => (
      <Switch>
        {routes.map((route, index) => (
          <Route
            key={index}
            exact
            path={`${props.match.url === '/' ? '' : props.match.url}${route.path}`}
            component={route.component}
          />
        ))}
        <Redirect
          to={{
            pathname: fallback,
          }}
        />
      </Switch>
    )}
  />
);

export default MultiRoute;
