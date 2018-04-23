const getIsLoggedIn = state => state.auth.isLoggedIn;

const getDatabase = state => state.auth.database;

export const selectors = { getIsLoggedIn, getDatabase };
