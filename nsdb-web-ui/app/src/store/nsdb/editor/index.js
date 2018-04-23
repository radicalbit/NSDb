import { types as editorTypes } from './types';
import { actions as editorActions } from './actions';
import { selectors as editorSelectors } from './selectors';
import editorEpic from './epic';
import editorReducer from './reducer';

export const types = {
  ...editorTypes,
};

export const actions = {
  ...editorActions,
};

export const selectors = {
  ...editorSelectors,
};

export const epic = editorEpic;

const reducer = editorReducer;

export default reducer;
