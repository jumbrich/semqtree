/**
 * 
 */
package de.ilmenau.datasum.config;



/**
 * @author hose
 *
 */
public class Parameters {
	
	/**
	 * defines whether to store DataPoints in the Buckets of QTree/MultiDimHistogram
	 * (used to calculate the quality of an index for evaluation) 
	 */
	public static boolean storeDataPointsInBuckets = false;
	
	/** determines the update strategy to use */
	public static UpdateStrategy updateStrategy = UpdateStrategy.NO_PROPAGATION;

	/**
	 * defines the minimum covered ratio used by the query feedback update strategy
	 * (covered capacity by query space cache / total capacity of the current query space)
	 */
	public static double minCoveredRatioInQuerySpaceCache = 0.9;
	
	
	/** enum of possible update strategies */
	public enum UpdateStrategy {
		/** do not propagate updates (disable propagation) and do not update the local index */
		NO_PROPAGATION,
		/** updates are sent immediately without checking conditions */
		IMMEDIATELY,
		/**
		 * same as IMMEDIATELY, but while message processing on a peer all UpdateEventMessages
		 * are evaluated.
		 * only used for evaluation as reference strategie to compare to.
		 */
		EVALUATION_REFERENCE,
		/**
		 * compute the change information for all neighbors on a peer together and
		 * send all recent updates to all neighbors simultaneously.
		 * <ul>
		 * 	<li>only based on optional thresholds</li>
		 * 	<li>only one copy of an index is needed for comparison!</li>
		 * </ul>
		 */
		SIMPLE_THRESHOLD,
		/**
		 * compute the change information for each neighbor on a peer separately and
		 * decide for each neighbor whether to send updates to it or not.
		 * <ul>
		 * 	<li>only based on optional thresholds</li>
		 * 	<li>for each neighbor a copy of an index is needed for comparison!</li>
		 * </ul>
		 */
		ADVANCED_THRESHOLD,
		/**
		 * every peer ask his neighbors periodically for updates.
		 * so the update propagation is not initiated by new updates on a peer!
		 */
		PROACTIVE,
		/**
		 * a <tt>SelectPop</tt> contains information about the amount of expected results.
		 * if the current results differ from the expected results then update are sent with
		 * the answer.
		 * if a peer did not send updates to a neighbor a long time then a UpdateEventMessage
		 * will be send.
		 * so the update propagation is not initiated by new updates on a peer!
		 */
		QUERY_ESTIMATION,
		/**
		 * the results of a <tt>SelectPop</tt> are used to update the index.
		 * so the update propagation is not initiated by new updates on a peer!
		 */
		QUERY_FEEDBACK
	}
}
