import { combineReducers } from 'redux';
import { combineEpics } from 'redux-observable';

import editorReducer, {
  types as editorTypes,
  actions as editorActions,
  selectors as editorSelectors,
  epic as editorEpic,
} from './editor';
import queryReducer, {
  types as queryTypes,
  actions as queryActions,
  selectors as querySelectors,
  epic as queryEpic,
} from './query';

export const types = {
  ...editorTypes,
  ...queryTypes,
};

export const actions = {
  ...editorActions,
  ...queryActions,
};

export const selectors = {
  ...editorSelectors,
  ...querySelectors,
};

export const epic = combineEpics(editorEpic, queryEpic);

const reducer = combineReducers({
  editor: editorReducer,
  query: queryReducer,
});

export default reducer;
