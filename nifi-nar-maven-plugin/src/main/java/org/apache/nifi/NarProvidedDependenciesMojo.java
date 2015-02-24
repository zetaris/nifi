/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.collection.CollectResult;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.graph.DependencyVisitor;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.util.artifact.JavaScopes;

/**
 * Packages the current project as an Apache NiFi Archive (NAR).
 *
 * The following code is derived from maven-dependencies-plugin and
 * maven-jar-plugin. The functionality of CopyDependenciesMojo and JarMojo was
 * simplified to the use case of NarMojo.
 *
 */
@Mojo(name = "provided-nar-dependencies", defaultPhase = LifecyclePhase.PACKAGE, threadSafe = false, requiresDependencyResolution = ResolutionScope.RUNTIME)
public class NarProvidedDependenciesMojo extends AbstractMojo {

    /**
     * The Maven project.
     */
    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    /**
     * If specified, this parameter will cause the dependency tree to be written
     * using the specified format. Currently supported format are:
     * <code>tree</code> or <code>pom</code>.
     */
    @Parameter(property = "mode", defaultValue = "tree")
    private String mode;
    
    /**
     * Skip plugin execution completely.
     */
    @Parameter(property = "skip", defaultValue = "false")
    private boolean skip;

    /**
     * The local artifact repository.
     */
    @Parameter(defaultValue = "${localRepository}", readonly = true)
    private ArtifactRepository localRepository;

    /**
     * The project's remote repositories to use for the resolution of plugins and their dependencies.
     *
     * @parameter default-value="${project.remotePluginRepositories}"
     * @readonly
     */
    @Component
    private List<RemoteRepository> remoteRepos;
    
    /**
     * The entry point to Aether, i.e. the component doing all the work.
     *
     * @component
     */
    @Component
    private RepositorySystem repoSystem;
 
    /*
     * @see org.apache.maven.plugin.Mojo#execute()
     */
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (isSkip()) {
            getLog().info("Skipping plugin execution");
            return;
        }

        try {
            // configure the session
            final DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
            final LocalRepository localRepo = new LocalRepository(localRepository.getBasedir());
            session.setLocalRepositoryManager(repoSystem.newLocalRepositoryManager(session, localRepo));

            final Artifact artifact = new DefaultArtifact(project.getArtifact().toString());

            final CollectRequest collectRequest = new CollectRequest();
            collectRequest.setRoot(new Dependency(artifact, JavaScopes.COMPILE));
            collectRequest.setRepositories(remoteRepos);

            final CollectResult collectResult = repoSystem.collectDependencies(session, collectRequest);
            
            if ("tree".equalsIgnoreCase(mode)) {
                collectResult.getRoot().accept(new TreeWriter());
            } else if ("pom".equalsIgnoreCase(mode)) {
                collectResult.getRoot().accept(new PomWriter());
            }
        } catch (final DependencyCollectionException dre) {
            throw new MojoExecutionException("Cannot build project dependency tree", dre);
        }
    }
    
    private boolean visitChildren(final Deque<DependencyNode> hierarchy) {
        if (hierarchy.size() == 1) {
            return true;
        } else if (hierarchy.size() == 2) {
            return "nar".equals(hierarchy.peekFirst().getArtifact().getExtension());
        } else {
            return false;
        }
    }
    
    private boolean visitSiblings(final Deque<DependencyNode> hierarchy) {
        if (hierarchy.size() == 2) {
            return "nar".equals(hierarchy.peekFirst().getArtifact().getExtension());
        } else {
            return false;
        }
    }
    
    private class TreeWriter implements DependencyVisitor {
        private final Deque<DependencyNode> hierarchy = new ArrayDeque<>();
        
        private boolean isLastChild(final DependencyNode parent, final DependencyNode child) {
            return parent != null && parent.getChildren().indexOf(child) == parent.getChildren().size() - 1;
        }
        
        @Override
        public boolean visitEnter(DependencyNode node) {
            // get the current parent for use later
            final DependencyNode parent = hierarchy.peek();
            
            // add this node
            hierarchy.push(node);
            
            // build the padding
            final StringBuilder pad = new StringBuilder();
            final Iterator<DependencyNode> iter = hierarchy.descendingIterator();
            
            // walk through the current hierarchy and add bars as appropriate
            // to properly show where the node originates
//            DependencyNode currentParent = iter.next();
//            while (iter.hasNext()) {
//                final DependencyNode currentNode = iter.next();
//                
//                if (!isLastChild(currentParent, currentNode)) {
//                    pad.append("|  ");
//                } else {
//                    pad.append("   ");
//                }
//                
//                currentParent = currentNode;
//            }
            
            for (int i = 0; i < hierarchy.size() - 1; i++) {
                pad.append("   ");
            }
            
            // if this is the last child use \ instead of +
            if (isLastChild(parent, node)) {
                pad.append("\\- ");
            } else {
                pad.append("+- ");
            }
            
            // log it
            getLog().info(pad + node.toString());
            
            return visitChildren(hierarchy);
        }

        @Override
        public boolean visitLeave(DependencyNode node) {
            hierarchy.pop();
            
            return visitSiblings(hierarchy);
        }
    }
    
    public class PomWriter implements DependencyVisitor {
        private final Deque<DependencyNode> hierarchy = new ArrayDeque<>();
        
        @Override
        public boolean visitEnter(DependencyNode node) {
            hierarchy.push(node);
            
            final Artifact artifact = node.getArtifact();

            if (!"nar".equals(artifact.getExtension())) {
                System.out.println("<dependency>");
                System.out.println("    <groupId>" + artifact.getGroupId() + "</groupId>");
                System.out.println("    <artifactId>" + artifact.getArtifactId() + "</artifactId>");
                System.out.println("</dependency>");
            }

            return visitChildren(hierarchy);
        }

        @Override
        public boolean visitLeave(DependencyNode node) {
            hierarchy.pop();
            
            return visitSiblings(hierarchy);
        }
    }
    
    /**
     * Gets the Maven project used by this mojo.
     *
     * @return the Maven project
     */
    public MavenProject getProject() {
        return project;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }
}
