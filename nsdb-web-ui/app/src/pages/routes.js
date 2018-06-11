import React from 'react';
import Loadable from 'react-loadable';

import NotFound from './NotFound';

const loginRoute = {
  path: '/login',
  component: Loadable({
    loader: () => import(/* webpackChunkName: "login" */ './Login'),
    loading: () => React.createElement('div', [], ['loading...']),
  }),
};

const restrictedRoutes = [
  {
    path: '/',
    component: Loadable({
      loader: () => import(/* webpackChunkName: "nsdb" */ './Nsdb'),
      loading: () => React.createElement('div', [], ['loading...']),
    }),
  },
  {
    path: '/404',
    component: NotFound,
  },
];

export { loginRoute, restrictedRoutes };
