import { types as TYPES } from './types';

/**
 * normal actions
 */

const removeResult = id => ({
  type: TYPES.RESULT_REMOVE,
  payload: {
    id,
  },
});

/**
 * async actions
 */

const fetchHistoricalQueryRequest = (id, query) => ({
  type: TYPES.HISTORICAL_QUERY_FETCH_REQUEST,
  payload: {
    id,
    query,
  },
});

const fetchHistoricalQuerySuccess = (id, response) => ({
  type: TYPES.HISTORICAL_QUERY_FETCH_SUCCESS,
  payload: {
    id,
    response,
  },
});

const fetchHistoricalQueryError = error => ({
  type: TYPES.HISTORICAL_QUERY_FETCH_ERROR,
  error,
});

const startRealtimeQuerySocket = (id, query) => ({
  type: TYPES.REALTIME_QUERY_START_SOCKET,
  payload: {
    id,
    query,
  },
});

const stopRealtimeQuerySocket = id => ({
  type: TYPES.REALTIME_QUERY_STOP_SOCKET,
  payload: {
    id,
  },
});

const onRealtimeQueryMessage = (id, message) => ({
  type: TYPES.REALTIME_QUERY_ON_MESSAGE,
  payload: {
    id,
    message,
  },
});

const onRealtimeQueryError = error => ({
  type: TYPES.REALTIME_QUERY_ON_ERROR,
  error,
});

export const actions = {
  removeResult,
  fetchHistoricalQueryRequest,
  fetchHistoricalQuerySuccess,
  fetchHistoricalQueryError,
  startRealtimeQuerySocket,
  stopRealtimeQuerySocket,
  onRealtimeQueryMessage,
  onRealtimeQueryError,
};
