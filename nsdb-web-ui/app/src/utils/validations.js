export const alwaysValid = value => ({
  error: null,
  warning: null,
  success: null,
});

export const notEmpty = value => ({
  error: !value ? "Field can't be empty" : null,
  warning: null,
  success: null,
});
