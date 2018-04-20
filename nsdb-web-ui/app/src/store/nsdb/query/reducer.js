import omit from 'lodash/omit';
import { types as TYPES } from './types';

const initialState = {
  results: {
    allIds: [],
    byId: {},
  },
  isFetchingHistoricalQuery: false,
};

function queryReducer(state = initialState, action) {
  switch (action.type) {
    /**
     * normal reducers
     */
    case TYPES.RESULT_REMOVE: {
      const { id } = action.payload;
      const allIds = state.results.allIds.filter(allId => allId !== id);
      const byId = omit(state.results.byId, [[id]]);
      const newResults = { allIds, byId };
      return { ...state, results: newResults };
    }
    /**
     * async reducers
     */
    case TYPES.HISTORICAL_QUERY_FETCH_REQUEST: {
      return { ...state, isFetchingHistoricalQuery: true };
    }
    case TYPES.HISTORICAL_QUERY_FETCH_SUCCESS: {
      const { id, response } = action.payload;
      const allIds = state.results.byId[id]
        ? state.results.allIds
        : state.results.allIds.concat(id);
      const byId = { ...state.results.byId, [id]: { id, records: response.records } };
      const newResults = { allIds, byId };
      return { ...state, results: newResults, isFetchingHistoricalQuery: false };
    }
    case TYPES.HISTORICAL_QUERY_FETCH_ERROR: {
      return { ...state, isFetchingHistoricalQuery: false };
    }
    case TYPES.REALTIME_QUERY_START_SOCKET: {
      return state;
    }
    case TYPES.REALTIME_QUERY_STOP_SOCKET: {
      return state;
    }
    case TYPES.REALTIME_QUERY_ON_MESSAGE: {
      const { id, message } = action.payload;
      const allIds = state.results.byId[id]
        ? state.results.allIds
        : state.results.allIds.concat(id);
      const byId = {
        ...state.results.byId,
        [id]: {
          id,
          records: state.results.byId[id]
            ? state.results.byId[id].records.concat(message.records)
            : [],
        },
      };
      const newResults = { allIds, byId };
      return { ...state, results: newResults };
    }
    case TYPES.REALTIME_QUERY_ON_ERROR: {
      return state;
    }
    default:
      return state;
  }
}

export default queryReducer;
