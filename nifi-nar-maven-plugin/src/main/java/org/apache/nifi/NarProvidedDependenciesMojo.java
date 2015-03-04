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

import java.util.HashMap;
import java.util.Map;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.manager.ArtifactHandlerManager;
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
import org.apache.maven.shared.dependency.tree.DependencyNode;
import org.apache.maven.shared.dependency.tree.DependencyTreeBuilder;
import org.apache.maven.shared.dependency.tree.DependencyTreeBuilderException;
import org.apache.maven.shared.dependency.tree.traversal.DependencyNodeVisitor;

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

    private static final String NAR = "nar";
    
    /**
     * The Maven project.
     */
    @Parameter( defaultValue = "${project}", readonly = true, required = true )
    private MavenProject project;

    /**
     * The dependency tree builder to use for verbose output.
     */
    @Component(hint = "default")
    private DependencyTreeBuilder dependencyTreeBuilder;

    /***
     * The {@link ArtifactHandlerManager} into which any extension {@link ArtifactHandler} instances should have been
     * injected when the extensions were loaded.
     */
    @Component
    private ArtifactHandlerManager artifactHandlerManager;
    
    /**
     * If specified, this parameter will cause the dependency tree to be written using the specified format. Currently
     * supported format are: <code>tree</code> or <code>pom</code>.
     */
    @Parameter(property = "mode", defaultValue = "tree")
    private String mode;

    /**
     * The local artifact repository.
     */
    @Parameter( defaultValue = "${localRepository}", readonly = true )
    private ArtifactRepository localRepository;

    /*
     * @see org.apache.maven.plugin.Mojo#execute()
     */
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            // find the nar dependency
            Artifact narArtifact = null;
            for (final Artifact artifact : project.getDependencyArtifacts()) {
                if (NAR.equals(artifact.getType())) {
                    // ensure the project doesn't have two nar dependencies
                    if (narArtifact != null) {
                        throw new MojoExecutionException("Project can only have one NAR dependency.");
                    }
                    
                    // record the nar dependency
                    narArtifact = artifact;
                }
            }
            
            // ensure there is a nar dependency
            if (narArtifact == null) {
                throw new MojoExecutionException("Project does not have any NAR dependencies.");
            }
            
            // get the artifact handler for excluding dependencies
            final ArtifactHandler narHandler = excludesDependencies(narArtifact);
            narArtifact.setArtifactHandler(narHandler);
            
            // nar artifacts by nature includes dependencies, however this prevents the
            // transitive dependencies from printing using tools like dependency:tree.
            // here we are overriding the artifact handler for all nars so the 
            // dependencies can be listed. this is important because nar dependencies
            // will be used as the parent classloader for this nar and seeing what
            // dependencies are provided is critical.
            final Map<String, ArtifactHandler> narHandlerMap = new HashMap<>();
            narHandlerMap.put(NAR, narHandler);
            artifactHandlerManager.addHandlers(narHandlerMap); 

            // get the dependency tree
            final DependencyNode root = dependencyTreeBuilder.buildDependencyTree(project, localRepository, null);

            // only show the tree for the provided nar dependencies
            DependencyNode narDependency = null;
            for (final DependencyNode dependency : root.getChildren()) {
                if (narArtifact.equals(dependency.getArtifact())) {
                    narDependency = dependency;
                    break;
                }
            }
            
            // ensure the nar dependency is found
            if (narDependency == null) {
                throw new MojoExecutionException("Unable to find NAR dependency.");
            }
            
            // write the appropriate output
            if ("tree".equals(mode)) {
                getLog().info("--- Provided NAR Dependencies ---\n\n" + narDependency.toString());
            } else if ("pom".equals(mode)) {
                final PomWriter out = new PomWriter();
                narDependency.accept(out);
                getLog().info("--- Provided NAR Dependencies ---\n\n" + out.toString());
            }
        } catch (DependencyTreeBuilderException exception) {
            throw new MojoExecutionException("Cannot build project dependency tree", exception);
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

    private ArtifactHandler excludesDependencies(final Artifact artifact) {
        final ArtifactHandler orig = artifact.getArtifactHandler();
        
        return new ArtifactHandler() {
            @Override
            public String getExtension() {
                return orig.getExtension();
            }

            @Override
            public String getDirectory() {
                return orig.getDirectory();
            }

            @Override
            public String getClassifier() {
                return orig.getClassifier();
            }

            @Override
            public String getPackaging() {
                return orig.getPackaging();
            }

            // mark dependencies has excluded so they will appear in tree listing
            
            @Override
            public boolean isIncludesDependencies() {
                return false;
            }

            @Override
            public String getLanguage() {
                return orig.getLanguage();
            }

            @Override
            public boolean isAddedToClasspath() {
                return orig.isAddedToClasspath();
            }
        };
    }

    private class PomWriter implements DependencyNodeVisitor {
        private final StringBuilder output = new StringBuilder();
        
        @Override
        public boolean visit(DependencyNode node) {
            final Artifact artifact = node.getArtifact();

            if (!NAR.equals(artifact.getType())) {
                output.append("<dependency>\n");
                output.append("    <groupId>").append(artifact.getGroupId()).append("</groupId>\n");
                output.append("    <artifactId>").append(artifact.getArtifactId()).append("</artifactId>\n");
                output.append("</dependency>\n");
            }

            return true;
        }

        @Override
        public boolean endVisit(DependencyNode node) {
            return true;
        }
        
        @Override
        public String toString() {
            return output.toString();
        }
    }
}
