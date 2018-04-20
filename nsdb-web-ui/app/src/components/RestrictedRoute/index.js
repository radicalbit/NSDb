import React from 'react';
import { Route, Redirect } from 'react-router';

const RestrictedRoute = ({ component: Component, render, isLoggedIn, fallback, ...rest }) => (
  <Route
    {...rest}
    render={props =>
      isLoggedIn ? (
        Component ? (
          <Component {...props} />
        ) : (
          render(props)
        )
      ) : (
        <Redirect
          to={{
            pathname: fallback,
            state: { from: props.location },
          }}
        />
      )
    }
  />
);

export default RestrictedRoute;
