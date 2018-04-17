import { createStore, applyMiddleware } from 'redux';
import { routerMiddleware as createRouterMiddleware } from 'react-router-redux';
import { createEpicMiddleware } from 'redux-observable';
import { createLogger } from 'redux-logger';
import { composeWithDevTools } from 'redux-devtools-extension';
import createReducer from './store/reducer';
import createEpic from './store/epic';

function createMiddlewares(history) {
  const loggerMiddleware = createLogger({
    level: 'info',
    collapsed: true,
  });
  const routerMiddleware = createRouterMiddleware(history);
  const epicMiddleware = createEpicMiddleware(createEpic());

  return {
    logger: loggerMiddleware,
    router: routerMiddleware,
    epic: epicMiddleware,
  };
}

export default function configureStore(history, initialState = {}) {
  const reducer = createReducer();
  const middlewares = createMiddlewares(history);
  const enhancer = composeWithDevTools(applyMiddleware(...Object.values(middlewares)));

  const store = createStore(reducer, initialState, enhancer);

  if (module.hot) {
    module.hot.accept('./store/reducer', () => {
      store.replaceReducer(createReducer());
    });
    module.hot.accept('./store/epic', () => {
      middlewares.epic.replaceEpic(createEpic());
    });
  }

  return store;
}
