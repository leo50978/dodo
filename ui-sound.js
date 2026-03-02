export function playUiSound(type = '') {
  const fn = window.playUiSound;
  if (typeof fn !== 'function') return;
  try {
    fn(type);
  } catch (error) {
    // Ignore audio runtime errors to keep UI interactions fluid.
  }
}
