const initialState = {
  isLoggedIn: false,
  database: 'db',
};

function authReducer(state = initialState, action) {
  switch (action.type) {
    default:
      return state;
  }
}

export default authReducer;
