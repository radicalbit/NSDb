import { combineReducers } from 'redux';
import { routerReducer } from 'react-router-redux';
import authReducer from './auth';
import nsdbReducer from './nsdb';

export default function createReducer() {
  return combineReducers({
    router: routerReducer,
    auth: authReducer,
    nsdb: nsdbReducer,
  });
}
