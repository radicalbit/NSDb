import { types as authTypes } from './types';
import { actions as authActions } from './actions';
import { selectors as authSelectors } from './selectors';
import authReducer from './reducer';

export const types = {
  ...authTypes,
};

export const actions = {
  ...authActions,
};

export const selectors = {
  ...authSelectors,
};

const reducer = authReducer;

export default reducer;
