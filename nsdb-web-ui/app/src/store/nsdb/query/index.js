import { types as queryTypes } from './types';
import { actions as queryActions } from './actions';
import { selectors as querySelectors } from './selectors';
import queryEpic from './epic';
import queryReducer from './reducer';

export const types = {
  ...queryTypes,
};

export const actions = {
  ...queryActions,
};

export const selectors = {
  ...querySelectors,
};

export const epic = queryEpic;

const reducer = queryReducer;

export default reducer;
