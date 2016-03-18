package spinat.oraclescripter;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.StatusCommand;
import org.eclipse.jgit.api.errors.GitAPIException;

import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryBuilder;

public class GitHelper {

    public static void createRepoInDir(File dir) throws IOException {
        try {
            Git git = Git.init().setDirectory(dir).setBare(false).call();
            git.close();
        } catch (GitAPIException ex) {
            throw new RuntimeException(ex);
        }
        checkForRepoInDir(dir);
    }

    public static void checkForRepoInDir(File dir) throws IOException {
        try {
            RepositoryBuilder rb = new RepositoryBuilder();
            File f = dir;
            rb.addCeilingDirectory(f);
            rb.setMustExist(true);
            rb.findGitDir(f);
            if (rb.getGitDir() == null) {
                 throw new RuntimeException("no repo found at: " + dir);
            }
            Repository r = rb.build();
            Git g = new Git(r);
            StatusCommand stc = g.status();
            Status st = stc.call();
        } catch (GitAPIException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void AddVersion(File dir, String msg) throws IOException {
        try {
            RepositoryBuilder rb = new RepositoryBuilder();
            File f = dir;
            rb.addCeilingDirectory(f);
            rb.setMustExist(true);
            rb.findGitDir(f);
            Repository r = rb.build();
            Git g = new Git(r);
            StatusCommand stc = g.status();
            Status st = stc.call();
            boolean someChange = false;
            System.out.println("# new Objects");
            Set<String> ut = st.getUntracked();
            if (!ut.isEmpty()) {
                AddCommand ac = g.add();
                ac.setUpdate(false);
                for (String s : ut) {
                    System.out.println(s);
                    ac.addFilepattern(s);
                }
                ac.call();
                someChange = true;
            }

            System.out.println("# removed Objects");
            Set<String> mi = st.getMissing();
            if (!mi.isEmpty()) {
                AddCommand ac = g.add();
                ac.setUpdate(true);

                for (String s : mi) {
                    System.out.println(s);
                    ac.addFilepattern(s);
                }
                ac.call();
                someChange = true;
            }

            System.out.println("# changed Objects");
            Set<String> mo = st.getModified();
            if (!mo.isEmpty()) {
                AddCommand ac = g.add();
                ac.setUpdate(true);

                for (String s : st.getModified()) {
                    System.out.println(s);
                    ac.addFilepattern(s);
                }
                ac.call();
                someChange = true;
            }
            if (someChange) {
                CommitCommand c = g.commit();
                if (msg == null) {
                    msg = "<>";
                }
                c.setMessage(msg);
                c.call();
            } else {
                System.out.println("nothing has changed");
            }
        } catch (GitAPIException e) {
            throw new RuntimeException(e);
        }

    }

}
