class CliContext:
    def __init__(self, description, action=None, sub_contexts=None):
        self.description = description
        self.action = action

        if self.action is None:
            self.sub_contexts = sub_contexts

    def run(self, program, args):
        if self.action is not None:
            return self.action(program, args)
        else:
            if len(args) == 0:
                self.print_help(program)
            else:
                cmd, sub_args = args[0], args[1:]

                if cmd == "-h" or cmd == "--help":
                    self.print_help(program)
                elif cmd in self.sub_contexts:
                    if self.sub_contexts[cmd].run(program + (cmd,), sub_args):
                        self.print_help(program)
                else:
                    print("Invalid command %s" % cmd)
                    self.print_help(program)

    def print_help(self, program):
        if self.sub_contexts is not None:
            program_str = " ".join(program)

            print("usage: %s <command> [<args>]" % program_str)

            if len(self.sub_contexts) > 0:
                print("\nCommands:")
                for (cmd, context) in self.sub_contexts.items():
                    print("%s - %s" % (cmd, context.description))

                print(
                    "\nFor detailed help of each command enter {0} <command> --help,\ne.g. {0} {1} --help".format(
                        program_str, next(iter(self.sub_contexts))
                    )
                )
