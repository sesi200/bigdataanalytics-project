/**
 * Copyright 2016 University of Zurich
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package examples;

import java.io.File;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import cc.kave.commons.model.events.CommandEvent;
import cc.kave.commons.model.events.IDEEvent;
import cc.kave.commons.model.events.completionevents.CompletionEvent;
import cc.kave.commons.model.events.testrunevents.TestRunEvent;
import cc.kave.commons.model.events.visualstudio.BuildEvent;
import cc.kave.commons.model.events.visualstudio.DebuggerEvent;
import cc.kave.commons.model.events.visualstudio.EditEvent;
import cc.kave.commons.model.events.visualstudio.WindowEvent;
import cc.kave.commons.model.ssts.ISST;
import cc.kave.commons.utils.io.IReadingArchive;
import cc.kave.commons.utils.io.ReadingArchive;

/**
 * Simple example that shows how the interaction dataset can be opened, all
 * users identified, and all contained events deserialized.
 */
public class GettingStarted {

	private String eventsDir;

	public GettingStarted(String eventsDir) {
		this.eventsDir = eventsDir;
	}

	public void run() {

		System.out.printf("looking (recursively) for events in folder %s\n", new File(eventsDir).getAbsolutePath());

		/*
		 * Each .zip that is contained in the eventsDir represents all events that we
		 * have collected for a specific user, the folder represents the first day when
		 * the user uploaded data.
		 */
		Set<String> userZips = IoHelper.findAllZips(eventsDir);

		for (String userZip : userZips) {
			System.out.printf("\n#### processing user zip: %s #####\n", userZip);
			processUserZip(userZip);
		}
	}

	private void processUserZip(String userZip) {
		int numProcessedEvents = 0;
		// open the .zip file ...
		try (IReadingArchive ra = new ReadingArchive(new File(eventsDir, userZip))) {
			// ... and iterate over content.
			// the iteration will stop after 200 events to speed things up.
			while (ra.hasNext() && (numProcessedEvents++ < 500)) {
				/*
				 * within the userZip, each stored event is contained as a single file that
				 * contains the Json representation of a subclass of IDEEvent.
				 */
				IDEEvent e = ra.getNext(IDEEvent.class);

				// the events can then be processed individually
				processEvent(e);
			}
		}
	}

	/*
	 * if you review the type hierarchy of IDEEvent, you will realize that several
	 * subclasses exist that provide access to context information that is specific
	 * to the event type.
	 * 
	 * To access the context, you should check for the runtime type of the event and
	 * cast it accordingly.
	 * 
	 * As soon as I have some more time, I will implement the visitor pattern to get
	 * rid of the casting. For now, this is recommended way to access the contents.
	 */

	private void processEvent(IDEEvent e) {

		if (e instanceof BuildEvent) {
			processBuildEvent((BuildEvent)e);
		} else if (e instanceof EditEvent) {
			processEditEvent((EditEvent)e);
		} else if (e instanceof TestRunEvent) {
			processTestEvent((TestRunEvent)e);
		}

	}
	
	private void processTestEvent(TestRunEvent e) {
		System.out.printf("test event found");
	}

	private void processEditEvent(EditEvent e) {
		System.out.printf("EditEvent detected\n");
	}

	private void processBuildEvent(BuildEvent e) {
		System.out.printf("Build pass: %s\n", e.Targets.stream().map(b->b.Successful).allMatch(p->p));
	}
}