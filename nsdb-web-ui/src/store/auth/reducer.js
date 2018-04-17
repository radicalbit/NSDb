const initialState = {
  isLoggedIn: false,
  database: 'certilogo',
};

function authReducer(state = initialState, action) {
  switch (action.type) {
    default:
      return state;
  }
}

export default authReducer;
