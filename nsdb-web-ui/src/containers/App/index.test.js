import React from 'react';
import ReactDOM from 'react-dom';
import { Provider } from 'react-redux';
import createHistory from 'history/createBrowserHistory';
import LocaleProvider from 'antd/lib/locale-provider';
import enUS from 'antd/lib/locale-provider/en_US';
import { loginRoute, restrictedRoutes } from '../../pages/routes';
import configureStore from '../../configureStore';

import App from '.';

import '../../assets/styles/theme.less';

const history = createHistory({ basename: process.env.PUBLIC_URL });
const store = configureStore(history);

it('renders without crashing', () => {
  const root = document.createElement('div');
  ReactDOM.render(
    <Provider store={store}>
      <LocaleProvider locale={enUS}>
        <App history={history} loginRoute={loginRoute} restrictedRoutes={restrictedRoutes} />
      </LocaleProvider>
    </Provider>,
    root
  );
  ReactDOM.unmountComponentAtNode(root);
});
