module.exports = () => {
  const errors = [];
  function handleError(error) {
    errors.push(error);
  }

  handleError.getErrors = () => errors.slice();
  return handleError;
};
