// DSH 0.2.2-rc.5 may append one empty seed marker when it opens a log.
// Other suffix events can be user writes from an independent DSH instance.
export function isProvisionalResumeMarker(event, firstSeq) {
  return event.seq === firstSeq && event.type === 'session/end-seed'
    && event.data !== null && typeof event.data === 'object' && !Array.isArray(event.data)
    && Object.keys(event.data).length === 0
    && Object.keys(event).every(key => key === 'type' || key === 'seq' || key === 'time' || key === 'data');
}
