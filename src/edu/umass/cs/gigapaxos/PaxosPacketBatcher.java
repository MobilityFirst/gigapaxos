/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */

package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAccept;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAcceptReply;
import edu.umass.cs.gigapaxos.paxospackets.BatchedCommit;
import edu.umass.cs.gigapaxos.paxospackets.BatchedPaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.ConsumerTask;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class PaxosPacketBatcher extends ConsumerTask<MessagingTask[]> {

	private final PaxosManager<?> paxosManager;
	private final HashMap<String, HashMap<Ballot, BatchedAcceptReply>> acceptReplies;
	private final HashMap<String, HashMap<Ballot, BatchedCommit>> commits;
	private final HashMap<String, HashMap<Ballot, BatchedAccept>> accepts;
	private final LinkedList<MessagingTask> requests;

	/**
	 * @param lock
	 * @param paxosManager
	 */
	private PaxosPacketBatcher(HashMapContainer lock,
			PaxosManager<?> paxosManager) {
		super(lock);
		this.acceptReplies = lock.acceptReplies;
		this.commits = lock.commits;
		this.accepts = lock.accepts;
		this.requests = lock.requests;
		this.paxosManager = paxosManager;
	}

	/**
	 * @param paxosManager
	 */
	public PaxosPacketBatcher(PaxosManager<?> paxosManager) {
		// we are using only one of the two structures to lock
		this(new HashMapContainer(), paxosManager);
	}

	// just to name the thread, otherwise super suffices
	public void start() {
		Thread me = (new Thread(this));
		me.setName(PaxosPacketBatcher.class.getSimpleName()
				+ this.paxosManager.getMyID());
		me.start();
	}

	@Override
	public void enqueueImpl(MessagingTask[] tasks) {
		for (MessagingTask task : tasks)
			if (task != null) {
				assert(!task.isEmptyMessaging() && task.msgs[0].getType()!=null) : task;
				switch (task.msgs[0].getType()) {
				case ACCEPT_REPLY:
					assert(task.msgs.length==1);
					this.enqueueImpl((AcceptReplyPacket) task.msgs[0]);
					break;
				case BATCHED_COMMIT:
					assert(task.msgs.length==1);
					this.enqueueImpl((BatchedCommit) task.msgs[0]);
					break;
				case BATCHED_ACCEPT:
					assert(task.msgs.length==1);
					this.enqueueImpl((BatchedAccept) task.msgs[0]);
					break;
				default:
					if(task.msgs[0] instanceof RequestPacket)
						this.enqueueImpl(task);
				}
			}
	}

	private boolean enqueueImpl(AcceptReplyPacket acceptReply) {
		if (!this.acceptReplies.containsKey(acceptReply.getPaxosID()))
			this.acceptReplies.put(acceptReply.getPaxosID(),
					new HashMap<Ballot, BatchedAcceptReply>());

		HashMap<Ballot, BatchedAcceptReply> arMap = this.acceptReplies
				.get(acceptReply.getPaxosID());
		boolean added = false;
		if (arMap.isEmpty())
			added = (arMap.put(acceptReply.ballot, new BatchedAcceptReply(
					acceptReply)) != null);
		else if (arMap.containsKey(acceptReply.ballot)) {
			added = arMap.get(acceptReply.ballot).addAcceptReply(acceptReply);
		} else {
			arMap.put(acceptReply.ballot, new BatchedAcceptReply(acceptReply));
		}

		return added;
	}

	private boolean enqueueImpl(BatchedCommit commit) {
		if (!this.commits.containsKey(commit.getPaxosID()))
			this.commits.put(commit.getPaxosID(),
					new HashMap<Ballot, BatchedCommit>());
		HashMap<Ballot, BatchedCommit> cMap = this.commits.get(commit
				.getPaxosID());
		boolean added = false;
		if (cMap.isEmpty())
			added = (cMap.put(commit.ballot, (commit)) != null);
		else if (cMap.containsKey(commit.ballot))
			added = cMap.get(commit.ballot).addBatchedCommit(commit);
		else
			added = (cMap.put(commit.ballot, (commit)) != null);

		return added;
	}

	private boolean enqueueImpl(BatchedAccept bAccept) {
		assert(bAccept.getPaxosID()!=null);
		if (!this.accepts.containsKey(bAccept.getPaxosID()))
			this.accepts.put(bAccept.getPaxosID(),
					new HashMap<Ballot, BatchedAccept>());
		HashMap<Ballot, BatchedAccept> aMap = this.accepts.get(bAccept
				.getPaxosID());
		boolean added = false;
		if (aMap.isEmpty())
			added = (aMap.put(bAccept.ballot, (bAccept)) != null);
		else if (aMap.containsKey(bAccept.ballot))
			added = aMap.get(bAccept.ballot).addBatchedAccept(bAccept);
		else
			added = (aMap.put(bAccept.ballot, (bAccept)) != null);

		return added;
		
	}
	
	private boolean enqueueImpl(MessagingTask requestMTask) {
		return this.requests.add(requestMTask);
	}
	
	@Override
	public MessagingTask[] dequeueImpl() {

		ArrayList<MessagingTask> pkts = new ArrayList<MessagingTask>();
		
		int lengthEstimate=0;
		
		PaxosPacket pp = null;
		MessagingTask mtask = null;
		while (lengthEstimate < NIOTransport.MAX_PAYLOAD_SIZE
				&& (pp = this.dequeueImplAR()) != null)
			if (pkts.add(new MessagingTask(((BatchedAcceptReply)pp).ballot.coordinatorID, pp)))
				lengthEstimate += RequestPacket.SIZE_ESTIMATE*((BatchedAcceptReply)pp).size();
		while (lengthEstimate < NIOTransport.MAX_PAYLOAD_SIZE
				&& (pp = this.dequeueImplC()) != null)
			if (pkts.add(new MessagingTask(((BatchedCommit)pp).getGroup(), pp)))
				lengthEstimate += RequestPacket.SIZE_ESTIMATE*((BatchedCommit)pp).size();
		while (lengthEstimate < NIOTransport.MAX_PAYLOAD_SIZE
				&& (pp = this.dequeueImplA()) != null)
			if (pkts.add(new MessagingTask(((BatchedAccept)pp).getGroup(), pp)))
				lengthEstimate += RequestPacket.SIZE_ESTIMATE*((BatchedAccept)pp).size();
		while (!this.requests.isEmpty()
				&& lengthEstimate < NIOTransport.MAX_PAYLOAD_SIZE
				&& (mtask = this.requests.removeFirst()) != null)
			if (pkts.add(mtask))
				lengthEstimate += ((RequestPacket) mtask.msgs[0]).lengthEstimate();

		return pkts.toArray(new MessagingTask[0]);
	}

	private PaxosPacket dequeueImplAR() {
		BatchedAcceptReply batchedAR = null;
		if (!this.acceptReplies.isEmpty()) {
			Map<Ballot, BatchedAcceptReply> arMap = this.acceptReplies.values()
					.iterator().next();
			assert (!arMap.isEmpty());
			batchedAR = arMap.values().iterator().next();
			arMap.remove(batchedAR.ballot);
			if (arMap.isEmpty())
				this.acceptReplies.remove(batchedAR.getPaxosID());
		}
		return batchedAR;
	}

	public String toString() {
		return this.getClass().getSimpleName() + this.paxosManager.getMyID();
	}

	private PaxosPacket dequeueImplC() {
		BatchedCommit batchedCommit = null;
		if (!this.commits.isEmpty()) {
			Map<Ballot, BatchedCommit> cMap = this.commits.values().iterator()
					.next();
			assert (!cMap.isEmpty());
			batchedCommit = cMap.values().iterator().next();
			cMap.remove(batchedCommit.ballot);
			if (cMap.isEmpty())
				this.commits.remove(batchedCommit.getPaxosID());
		}
		return batchedCommit;

	}

	private PaxosPacket dequeueImplA() {
		BatchedAccept batchedAccept = null;
		if (!this.accepts.isEmpty()) {
			Map<Ballot, BatchedAccept> aMap = this.accepts.values().iterator()
					.next();
			assert (!aMap.isEmpty());
			batchedAccept = aMap.values().iterator().next();
			aMap.remove(batchedAccept.ballot);
			if (aMap.isEmpty())
				this.accepts.remove(batchedAccept.getPaxosID());
		}
		return batchedAccept;
	}

	private void send(MessagingTask mtask) {
		try {
			this.paxosManager.send(mtask, false, false);
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}
	
	private static final boolean BATCH_ACROSS_GROUPS = Config.getGlobalBoolean(PC.BATCH_ACROSS_GROUPS);
	private static final int MIN_PP_BATCH_SIZE = Config.getGlobalInt(PC.MIN_PP_BATCH_SIZE);

	@Override
	public void process(MessagingTask[] tasks) {
		if (BATCH_ACROSS_GROUPS && tasks.length>MIN_PP_BATCH_SIZE)
			for (MessagingTask mtask : this.batch(tasks))
				this.send(mtask);
		else 
			for (MessagingTask mtask : tasks)
				this.send(mtask);
	}
	
	private static final boolean ENABLE_INSTRUMENTATION = Config.getGlobalBoolean(PC.ENABLE_INSTRUMENTATION);
	private MessagingTask[] batch(MessagingTask[] mtasks) {
		Map<Set<Integer>, BatchedPaxosPacket> grouped = new LinkedHashMap<Set<Integer>, BatchedPaxosPacket>();
		for (MessagingTask mtask : mtasks) {
			if (mtask == null || mtask.isEmptyMessaging())
				continue;
			{
				Set<Integer> group = Util.arrayToIntSet(mtask.recipients);
				assert (group != null);
				if (!grouped.containsKey(group))
					grouped.put(group, new BatchedPaxosPacket(mtask.msgs));
				else
					grouped.get(group).append(mtask.msgs);
				assert (grouped.get(group).size() > 0);
				if(ENABLE_INSTRUMENTATION && Util.oneIn(10)) DelayProfiler
						.updateMovAvg("#ppbatched", grouped.get(group).size());
			}
		}
		ArrayList<MessagingTask> batchedMTasks = new ArrayList<MessagingTask>();
		for (Set<Integer> group : grouped.keySet())
			batchedMTasks.add(new MessagingTask(Util.setToIntArray(group),
					grouped.get(group)));
		return batchedMTasks.toArray(new MessagingTask[0]);

	}
	

	private static boolean BATCHED_REQUESTS = 
			//Config.getGlobalBoolean(PC.DIGEST_REQUESTS) && 
			Config.getGlobalBoolean(PC.BATCHED_REQUESTS);

	private static boolean BATCHED_ACCEPTS = Config
			.getGlobalBoolean(PC.DIGEST_REQUESTS) && !Config.getGlobalBoolean(PC.FLIP_BATCHED_ACCEPTS);

	private static boolean BATCHED_ACCEPT_REPLIES = Config
			.getGlobalBoolean(PC.BATCHED_ACCEPT_REPLIES);

	private static boolean BATCHED_COMMITS = Config
			.getGlobalBoolean(PC.BATCHED_COMMITS);

	private static boolean isUnbatchableDecision(MessagingTask mtask) {
		return !BATCHED_COMMITS
				&& mtask.msgs[0].getType() == PaxosPacketType.DECISION;
	}

	private static boolean isUnbatchableAcceptReplies(MessagingTask mtask) {
		return !BATCHED_ACCEPT_REPLIES
				&& mtask.msgs[0].getType() == PaxosPacketType.ACCEPT_REPLY;
	}

	private static boolean isUnbatchableAccepts(MessagingTask mtask) {
		return !BATCHED_ACCEPTS
				&& mtask.msgs[0].getType() == PaxosPacketType.ACCEPT;
	}
	private static boolean isUnbatchableRequests(MessagingTask mtask) {
		return (!BATCHED_REQUESTS)
				&& (mtask.msgs[0].getType() == PaxosPacketType.REQUEST
				|| mtask.msgs[0].getType() == PaxosPacketType.PROPOSAL);
	}

	/*
	 * Coalesces non-local messaging tasks and returns either the local part of
	 * the messaging task or the input mtask if unable to coalesce. The sender
	 * is expected to direct-send the returned messaging task. 
	 */
	private static final boolean SHORT_CIRCUIT_LOCAL = Config.getGlobalBoolean(PC.SHORT_CIRCUIT_LOCAL);
	protected MessagingTask coalesce(MessagingTask mtask) {
		if (mtask == null || mtask.isEmptyMessaging()
				|| (allLocal(mtask) && SHORT_CIRCUIT_LOCAL)
				|| isUnbatchableAcceptReplies(mtask)
				|| isUnbatchableDecision(mtask) 
				|| isUnbatchableAccepts(mtask)
			|| isUnbatchableRequests(mtask))
			return mtask;
		
		MessagingTask nonLocal = MessagingTask.getNonLoopback(mtask,
				this.paxosManager.getMyID());
		if (nonLocal == null || nonLocal.isEmptyMessaging())
			return mtask;
		MessagingTask local = MessagingTask.getLoopback(mtask,
				this.paxosManager.getMyID());
		if(local == null || local.isEmptyMessaging()) ; // no-op

		boolean isAccReply = allPositiveAcceptReplies(mtask), isCommit = allCoalescableDecisions(mtask),
				//
				isAccept = allCoalescableAccepts(mtask), isRequest = allCoalescableRequests(mtask);
		if (!isAccReply && !isCommit && !isAccept && !isRequest)
			return mtask;

		if(SHORT_CIRCUIT_LOCAL) mtask = nonLocal;
		
		// use batched mtask as "local" NIO is worse for large requests
		if (isAccReply) {
			assert(mtask.msgs.length==1);
			this.enqueue(mtask.toArray());
		} else if (isCommit) {
			this.enqueue(this.fuseBatchedCommits(mtask));
		} else if (isAccept) {
			assert(mtask.msgs.length==1);
			this.enqueue(new MessagingTask(mtask.recipients, new BatchedAccept(
					(AcceptPacket) mtask.msgs[0],
					Util.arrayToIntSet((SHORT_CIRCUIT_LOCAL ? Util.filter(
							mtask.recipients, this.paxosManager.getMyID())
							: mtask.recipients)))).toArray());
		} else if (isRequest) {
			this.enqueue(mtask.toArray());
		} else assert(false);
		return SHORT_CIRCUIT_LOCAL ? local : null;  //local could still be null
	}
	
	private MessagingTask[] fuseBatchedCommits(MessagingTask mtask) {
		ArrayList<MessagingTask> batchedCommits = new ArrayList<MessagingTask>();
		BatchedCommit batchedCommit = new BatchedCommit(
				(PValuePacket) mtask.msgs[0],
				Util.arrayToIntSet((SHORT_CIRCUIT_LOCAL ? Util.filter(
						mtask.recipients, this.paxosManager.getMyID())
						: mtask.recipients)));
		batchedCommits.add(new MessagingTask(batchedCommit.getGroup(), batchedCommit));
		// add rest into first, so index starts from 1
		for (int i = 1; i < mtask.msgs.length; i++) {
			if (batchedCommit.ballot
					.compareTo(((PValuePacket) mtask.msgs[i]).ballot) == 0)
				batchedCommit.addCommit((PValuePacket) mtask.msgs[i]);
			else {
				// can only add same ballot decisions
				//this.enqueue(batchedCommit.toSingletonArray());
				batchedCommit = new BatchedCommit(
						(PValuePacket) mtask.msgs[i],
						Util.arrayToIntSet((SHORT_CIRCUIT_LOCAL ? Util.filter(
								mtask.recipients, this.paxosManager.getMyID())
								: mtask.recipients)));
				batchedCommits.add(new MessagingTask(batchedCommit.getGroup(), batchedCommit));
			}
		}
		return batchedCommits.toArray(new MessagingTask[0]);
	}

	private boolean allLocal(MessagingTask mtask) {
		for (int recipient : mtask.recipients)
			if (recipient != this.paxosManager.getMyID())
				return false;
		return true;
	}

	private static boolean allPositiveAcceptReplies(MessagingTask mtask) {
		for (PaxosPacket ppkt : mtask.msgs)
			if (!(ppkt instanceof AcceptReplyPacket && ((AcceptReplyPacket)ppkt).isCoalescable() 
					// only positive accept replies are batchable
					&& ((AcceptReplyPacket)ppkt).ballot.coordinatorID==mtask.recipients[0]))
				return false;
		return true;
	}

	private static boolean allCoalescableDecisions(MessagingTask mtask) {
		for (PaxosPacket ppkt : mtask.msgs)
			if (!(ppkt instanceof PValuePacket && ((PValuePacket) ppkt)
					.isCoalescable()))
				return false;
		return true;
	}
	private static boolean allCoalescableAccepts(MessagingTask mtask) {
		for (PaxosPacket ppkt : mtask.msgs)
			if (!(ppkt instanceof AcceptPacket && !((AcceptPacket) ppkt)
					.hasRequestValue())) 
				return false;
		return true;
	}

	private static boolean allCoalescableRequests(MessagingTask mtask) {
		for (PaxosPacket ppkt : mtask.msgs)
			if (!(ppkt instanceof RequestPacket
					&& ((RequestPacket) ppkt).batchSize() == 0 
					))
				return false;
		return true;
	}

	// used by paxos manager to garbage collect decisions
	protected static Map<String, Integer> getMaxLoggedDecisionMap(
			MessagingTask[] mtasks) {
		HashMap<String, Integer> maxLoggedDecisionMap = new HashMap<String, Integer>();
		for (MessagingTask mtask : mtasks) {
			PValuePacket loggedDecision = null;
			if (mtask.isEmptyMessaging()
					&& mtask instanceof LogMessagingTask
					&& (loggedDecision = (PValuePacket) ((LogMessagingTask) mtask).logMsg) != null
					&& loggedDecision.getType()
							.equals(PaxosPacketType.DECISION)) {
				int loggedDecisionSlot = loggedDecision.slot;
				String paxosID = loggedDecision.getPaxosID();
				if (!maxLoggedDecisionMap.containsKey(paxosID))
					maxLoggedDecisionMap.put(paxosID, loggedDecisionSlot);
				else if (maxLoggedDecisionMap.get(paxosID) - loggedDecisionSlot < 0)
					maxLoggedDecisionMap.put(paxosID, loggedDecisionSlot);
			}
		}
		return maxLoggedDecisionMap;
	}

	// entire piece of stupidity needed only for the isEmpty() method
	static class HashMapContainer implements
			Collection<HashMap<String, HashMap<Ballot, ?>>> {
		private final HashMap<String, HashMap<Ballot, BatchedAcceptReply>> acceptReplies = new HashMap<String, HashMap<Ballot, BatchedAcceptReply>>();
		private final HashMap<String, HashMap<Ballot, BatchedCommit>> commits = new HashMap<String, HashMap<Ballot, BatchedCommit>>();
		private final HashMap<String, HashMap<Ballot, BatchedAccept>> accepts = new HashMap<String, HashMap<Ballot, BatchedAccept>>();
		private final LinkedList<MessagingTask> requests = new LinkedList<MessagingTask>();

		@Override
		public int size() {
			return acceptReplies.size() + commits.size() + this.accepts.size() + requests.size() ;
		}

		@Override
		public boolean isEmpty() {
			return size() == 0;
		}

		@Override
		public boolean contains(Object o) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public Iterator<HashMap<String, HashMap<Ballot, ?>>> iterator() {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public Object[] toArray() {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public <T> T[] toArray(T[] a) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean add(HashMap<String, HashMap<Ballot, ?>> e) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean remove(Object o) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean addAll(
				Collection<? extends HashMap<String, HashMap<Ballot, ?>>> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new RuntimeException("Not implemented");
		}

		@Override
		public void clear() {
			throw new RuntimeException("Not implemented");
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Ballot b1 = new Ballot(23, 456);
		Ballot b2 = new Ballot(23, 456);
		assert (b1.equals(b2));
		Set<Ballot> bset = new HashSet<Ballot>();
		bset.add(b1);
		assert (bset.contains(b1));
		assert (bset.contains(b2));
		bset.add(b2);
		assert (bset.size() == 1) : bset.size();
	}

}
